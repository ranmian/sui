// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anemo::{Network as AnemoNetwork, PeerId, Request as AnemoRequest};
use anyhow::{Context, Result, anyhow};
use bytes::{Buf, BufMut};
use clap::Parser;
use futures::StreamExt;
use multiaddr::{Multiaddr, Protocol};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::marker::PhantomData;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use sui_data_store::{
    EpochData, EpochStore, EpochStoreWriter, ObjectKey as HistoryObjectKey,
    ObjectStore as HistoryObjectStore, ObjectStoreWriter, VersionQuery,
    node::Node,
    stores::{DataStore, InMemoryStore},
};
use sui_network::state_sync::StateSyncClient;
use sui_network::tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use sui_network::tonic::codegen::http::uri::PathAndQuery;
use sui_network::tonic::transport::{Channel, Endpoint};
use sui_network::tonic::{Request, Status};
use sui_replay_2::local_replay::{
    LocalReplayEngine, PrefetchRequest, RemoteBackedMemoryStore, ReplayStateStore,
    prefetch_requests_for_transaction,
};
use sui_rpc::field::{FieldMask, FieldMaskUtil};
use sui_rpc::proto::sui::rpc::v2::{
    self as rpc, BatchGetObjectsRequest, GetObjectRequest, SubscribeCheckpointsRequest,
    ledger_service_client::LedgerServiceClient,
    subscription_service_client::SubscriptionServiceClient,
};
use sui_rpc_api::client::Client as RpcClient;
use sui_types::base_types::ObjectID;
use sui_types::effects::{TransactionEffects, TransactionEffectsAPI};
use sui_types::message_envelope::Message;
use sui_types::messages_checkpoint::{
    CheckpointDigest, CheckpointRequestV2, CheckpointResponseV2, CheckpointSummary,
    CheckpointSummaryResponse, VersionedFullCheckpointContents,
};
use sui_types::object::Object as SuiObject;
use sui_types::transaction::TransactionData;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout};

const SYSTEM_STATE_RPC_URL: &str = "https://fullnode.mainnet.sui.io:443";
const FULLNODE_GRPC_URL: &str = "https://fullnode.mainnet.sui.io:443";
const TARGET_POOL_OBJECT_TYPE_PREFIX: &str =
    "0x1eabed72c53feb3805120a081dc15963c204dc8d091542592abaf7a35689b2fb::pool::Pool<";
const FASTEST_VALIDATOR_COUNT: usize = 5;
const CONTENTS_RACE_WIDTH: usize = 4;
const MAX_SUBSCRIPTION_MESSAGE_SIZE: usize = 64 * 1024 * 1024;
const OBSERVATION_CHANNEL_CAPACITY: usize = 4096;
const POLL_INTERVAL: Duration = Duration::from_millis(100);
const TCP_PROBE_TIMEOUT: Duration = Duration::from_secs(3);
const VALIDATOR_GRPC_TIMEOUT: Duration = Duration::from_secs(4);
const STATE_SYNC_TIMEOUT: Duration = Duration::from_secs(4);
const P2P_CONNECT_TIMEOUT: Duration = Duration::from_secs(4);
const SUBSCRIPTION_RETRY_DELAY: Duration = Duration::from_secs(1);
const OFFICIAL_STATE_SYNC_SEEDS: &[(&str, &str, &str)] = &[
    (
        "ewr-00.mainnet.sui.io",
        "/dns/ewr-00.mainnet.sui.io/udp/8084",
        "c7bf6cb93ca8fdda655c47ebb85ace28e6931464564332bf63e27e90199c50ee",
    ),
    (
        "ewr-01.mainnet.sui.io",
        "/dns/ewr-01.mainnet.sui.io/udp/8084",
        "3227f8a05f0faa1a197c075d31135a366a1c6f3d4872cb8af66c14dea3e0eb66",
    ),
    (
        "lhr-00.mainnet.sui.io",
        "/dns/lhr-00.mainnet.sui.io/udp/8084",
        "c619a5e0f8f36eac45118c1f8bda28f0f508e2839042781f1d4a9818043f732c",
    ),
    (
        "sui-mainnet-ssfn-ashburn-na.overclock.run",
        "/dns/sui-mainnet-ssfn-ashburn-na.overclock.run/udp/8084",
        "5ff8461ab527a8f241767b268c7aaf24d0312c7b923913dd3c11ee67ef181e45",
    ),
];

#[derive(Debug, Parser)]
#[command(name = "pending-local-replay-compare")]
#[command(about = "Compare pending checkpoint local replay timing with SubscribeCheckpoints")]
struct Args {
    #[arg(long, default_value = SYSTEM_STATE_RPC_URL)]
    system_state_rpc_url: String,

    #[arg(long, default_value = FULLNODE_GRPC_URL)]
    fullnode_grpc_url: String,
}

#[derive(Debug, Deserialize)]
struct JsonRpcResponse<T> {
    result: Option<T>,
    error: Option<JsonRpcError>,
}

#[derive(Debug, Deserialize)]
struct JsonRpcError {
    code: i64,
    message: String,
    #[serde(default)]
    data: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct CommitteeResult {
    #[serde(default, alias = "activeValidators")]
    active_validators: Vec<ValidatorEntry>,
}

#[derive(Debug, Deserialize)]
struct ValidatorEntry {
    #[serde(default)]
    metadata: Option<ValidatorMetadata>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default, alias = "netAddress", alias = "networkAddress")]
    network_address: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ValidatorMetadata {
    name: String,
    #[serde(alias = "netAddress", alias = "networkAddress")]
    network_address: String,
}

#[derive(Debug, Clone)]
struct ValidatorEndpoint {
    name: String,
    grpc_url: String,
    host: String,
    port: u16,
}

#[derive(Debug, Clone)]
struct LatencySample {
    validator: ValidatorEndpoint,
    latency: Duration,
}

#[derive(Debug, Clone)]
struct ValidatorGrpcClient {
    channel: Channel,
}

#[derive(Debug, Default)]
struct ValidatorRuntimeStats {
    avg_checkpoint_rtt_ms: Option<f64>,
    checkpoint_successes: u64,
    checkpoint_failures: u64,
}

#[derive(Debug, Clone)]
struct FastValidator {
    sample: LatencySample,
    grpc: ValidatorGrpcClient,
    stats: Arc<RwLock<ValidatorRuntimeStats>>,
}

#[derive(Debug, Default)]
struct SeedRuntimeStats {
    avg_contents_rtt_ms: Option<f64>,
    contents_successes: u64,
    contents_failures: u64,
}

#[derive(Debug, Clone)]
struct StateSyncSeed {
    label: String,
    peer_id: PeerId,
    address: anemo::types::Address,
    stats: Arc<RwLock<SeedRuntimeStats>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ObservationSource {
    PendingLocalReplay,
    CheckpointSubscription,
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
struct ObjectVersionKey {
    object_id: ObjectID,
    version: u64,
}

#[derive(Debug, Clone)]
struct ObjectObservation {
    source: ObservationSource,
    checkpoint_sequence: u64,
    seen_at_ms: u128,
    path_discovered_at_ms: u128,
    object_key: ObjectVersionKey,
    object_digest: String,
    object_type: String,
    owner: String,
    object_bcs_len: usize,
    move_contents_len: Option<usize>,
    source_label: String,
}

#[derive(Debug, Default)]
struct ObservationPair {
    fast: Option<ObjectObservation>,
    subscription: Option<ObjectObservation>,
}

#[derive(Debug)]
struct BcsEncoder<T>(PhantomData<T>);

impl<T: Serialize> Encoder for BcsEncoder<T> {
    type Item = T;
    type Error = Status;

    fn encode(&mut self, item: Self::Item, buf: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        bcs::serialize_into(&mut buf.writer(), &item).map_err(|e| Status::internal(e.to_string()))
    }
}

#[derive(Debug)]
struct BcsDecoder<U>(PhantomData<U>);

impl<U: serde::de::DeserializeOwned> Decoder for BcsDecoder<U> {
    type Item = U;
    type Error = Status;

    fn decode(&mut self, buf: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        if !buf.has_remaining() {
            return Ok(None);
        }

        let chunk = buf.chunk();
        let item = bcs::from_bytes(chunk).map_err(|e| Status::internal(e.to_string()))?;
        buf.advance(chunk.len());
        Ok(Some(item))
    }
}

#[derive(Debug, Clone)]
struct BcsCodec<T, U>(PhantomData<(T, U)>);

impl<T, U> Default for BcsCodec<T, U> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<T, U> Codec for BcsCodec<T, U>
where
    T: Serialize + Send + 'static,
    U: serde::de::DeserializeOwned + Send + 'static,
{
    type Encode = T;
    type Decode = U;
    type Encoder = BcsEncoder<T>;
    type Decoder = BcsDecoder<U>;

    fn encoder(&mut self) -> Self::Encoder {
        BcsEncoder(PhantomData)
    }

    fn decoder(&mut self) -> Self::Decoder {
        BcsDecoder(PhantomData)
    }
}

impl ValidatorGrpcClient {
    async fn connect(endpoint: &str) -> Result<Self> {
        let channel = Endpoint::from_shared(endpoint.to_string())
            .with_context(|| format!("invalid validator endpoint `{endpoint}`"))?
            .connect_timeout(Duration::from_secs(3))
            .timeout(VALIDATOR_GRPC_TIMEOUT)
            .tcp_nodelay(true)
            .connect()
            .await
            .with_context(|| format!("failed to connect validator `{endpoint}`"))?;
        Ok(Self { channel })
    }

    async fn checkpoint_v2(&self, request: CheckpointRequestV2) -> Result<CheckpointResponseV2> {
        let mut grpc = sui_network::tonic::client::Grpc::new(self.channel.clone());
        grpc.ready()
            .await
            .map_err(|e| anyhow!("validator gRPC not ready: {e}"))?;

        let path = PathAndQuery::from_static("/sui.validator.Validator/CheckpointV2");
        let codec = BcsCodec::<CheckpointRequestV2, CheckpointResponseV2>::default();

        let response = grpc
            .unary(Request::new(request), path, codec)
            .await
            .map_err(|status| {
                anyhow!(
                    "checkpoint_v2 RPC failed ({:?}): {}",
                    status.code(),
                    status.message()
                )
            })?;

        Ok(response.into_inner())
    }
}

struct HybridHistoryStore {
    fullnode_grpc_url: String,
    cache: InMemoryStore,
    gql: DataStore,
}

impl HybridHistoryStore {
    fn new(fullnode_grpc_url: String) -> Result<Self> {
        Ok(Self {
            fullnode_grpc_url,
            cache: InMemoryStore::new(Node::Mainnet),
            gql: DataStore::new(Node::Mainnet, env!("CARGO_PKG_VERSION"))?,
        })
    }

    async fn fetch_exact_version_objects(
        fullnode_grpc_url: String,
        keys: Vec<HistoryObjectKey>,
    ) -> Result<Vec<Option<(SuiObject, u64)>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let read_mask = FieldMask::from_paths([rpc::Object::path_builder().bcs().finish()]);
        let requests = keys
            .iter()
            .map(|key| {
                let mut request = GetObjectRequest::new(&key.object_id.into());
                let VersionQuery::Version(version) = key.version_query else {
                    unreachable!("exact-version fetch received non-version key");
                };
                request.version = Some(version);
                request
            })
            .collect::<Vec<_>>();

        let response = LedgerServiceClient::connect(fullnode_grpc_url.clone())
            .await
            .with_context(|| format!("failed to connect ledger client to {fullnode_grpc_url}"))?
            .max_decoding_message_size(MAX_SUBSCRIPTION_MESSAGE_SIZE)
            .batch_get_objects(
                BatchGetObjectsRequest::default()
                    .with_requests(requests)
                    .with_read_mask(read_mask),
            )
            .await
            .context("BatchGetObjects failed while fetching exact object versions")?
            .into_inner();

        let mut results = Vec::with_capacity(response.objects.len());
        for object_result in response.objects {
            match object_result.result {
                Some(rpc::get_object_result::Result::Object(object)) => {
                    let object: SuiObject = object.bcs().deserialize().with_context(|| {
                        format!(
                            "failed to deserialize object bcs for object {}",
                            object.object_id()
                        )
                    })?;
                    let version = object.version().value();
                    results.push(Some((object, version)));
                }
                Some(rpc::get_object_result::Result::Error(_)) | None | Some(_) => {
                    results.push(None);
                }
            }
        }

        Ok(results)
    }
}

impl EpochStore for HybridHistoryStore {
    fn epoch_info(&self, epoch: u64) -> Result<Option<EpochData>> {
        if let Some(epoch_data) = self.cache.epoch_info(epoch)? {
            return Ok(Some(epoch_data));
        }

        let epoch_data = self.gql.epoch_info(epoch)?;
        if let Some(epoch_data) = &epoch_data {
            self.cache.write_epoch_info(epoch, epoch_data.clone())?;
        }
        Ok(epoch_data)
    }

    fn protocol_config(
        &self,
        epoch: u64,
    ) -> Result<Option<sui_types::supported_protocol_versions::ProtocolConfig>> {
        if let Some(config) = self.cache.protocol_config(epoch)? {
            return Ok(Some(config));
        }
        self.gql.protocol_config(epoch)
    }
}

impl HistoryObjectStore for HybridHistoryStore {
    fn get_objects(&self, keys: &[HistoryObjectKey]) -> Result<Vec<Option<(SuiObject, u64)>>> {
        let mut results = self.cache.get_objects(keys)?;
        let mut exact_indices = Vec::new();
        let mut exact_keys = Vec::new();
        let mut historical_indices = Vec::new();
        let mut historical_keys = Vec::new();

        for (index, (key, result)) in keys.iter().zip(results.iter()).enumerate() {
            if result.is_some() {
                continue;
            }

            match key.version_query {
                VersionQuery::Version(_) => {
                    exact_indices.push(index);
                    exact_keys.push(key.clone());
                }
                VersionQuery::RootVersion(_) | VersionQuery::AtCheckpoint(_) => {
                    historical_indices.push(index);
                    historical_keys.push(key.clone());
                }
            }
        }

        if !exact_keys.is_empty() {
            let fetched = block_on_runtime(Self::fetch_exact_version_objects(
                self.fullnode_grpc_url.clone(),
                exact_keys.clone(),
            ))?;

            for ((index, key), object) in exact_indices.into_iter().zip(exact_keys).zip(fetched) {
                if let Some((object, actual_version)) = object {
                    self.cache
                        .write_object(&key, object.clone(), actual_version)?;
                    results[index] = Some((object, actual_version));
                }
            }
        }

        if !historical_keys.is_empty() {
            let fetched = self.gql.get_objects(&historical_keys)?;
            for ((index, key), object) in historical_indices
                .into_iter()
                .zip(historical_keys)
                .zip(fetched)
            {
                if let Some((object, actual_version)) = object {
                    self.cache
                        .write_object(&key, object.clone(), actual_version)?;
                    results[index] = Some((object, actual_version));
                }
            }
        }

        Ok(results)
    }
}

#[derive(Default)]
struct EngineCache {
    engines: BTreeMap<u64, LocalReplayEngine>,
}

impl EngineCache {
    fn get_or_create<'a>(
        &'a mut self,
        epoch: u64,
        history_store: &dyn EpochStore,
    ) -> Result<&'a LocalReplayEngine> {
        if let std::collections::btree_map::Entry::Vacant(entry) = self.engines.entry(epoch) {
            let epoch_data = history_store
                .epoch_info(epoch)?
                .ok_or_else(|| anyhow!("missing epoch data for epoch {epoch}"))?;
            let protocol_config = history_store
                .protocol_config(epoch)?
                .ok_or_else(|| anyhow!("missing protocol config for epoch {epoch}"))?;
            let engine = LocalReplayEngine::new(
                protocol_config,
                epoch,
                epoch_data.start_timestamp,
                epoch_data.rgp,
            )?;
            entry.insert(engine);
        }

        self.engines
            .get(&epoch)
            .ok_or_else(|| anyhow!("engine cache missing epoch {epoch} after insertion"))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let rpc_client = reqwest::Client::builder().no_proxy().build()?;

    let chain_identifier = fetch_chain_identifier(&rpc_client, &args.system_state_rpc_url).await?;
    let p2p_server_name = format!("sui-{chain_identifier}");
    let (startup_checkpoint_floor, startup_digest) =
        fetch_startup_checkpoint_baseline(&args.fullnode_grpc_url).await?;

    let validators = fetch_active_validators(&rpc_client, &args.system_state_rpc_url).await?;
    let endpoints = build_validator_endpoints(validators)?;
    if endpoints.is_empty() {
        return Err(anyhow!("no usable validator endpoints found"));
    }

    let latency_samples = probe_validators(endpoints).await;
    if latency_samples.is_empty() {
        return Err(anyhow!(
            "all validator endpoints are unreachable (TCP probe failed)"
        ));
    }

    let fast_validators = connect_fast_validators(latency_samples).await?;
    if fast_validators.is_empty() {
        return Err(anyhow!("no validator is usable for pending fast path"));
    }

    let network = build_p2p_network(&p2p_server_name)?;
    let state_sync_seeds = build_state_sync_seeds()?;
    connect_state_sync_seeds(&network, &state_sync_seeds).await;

    let history_store = Arc::new(HybridHistoryStore::new(args.fullnode_grpc_url.clone())?);

    println!(
        "using {} validator(s) for pending replay:",
        fast_validators.len()
    );
    for validator in &fast_validators {
        println!(
            "  - {:<24} grpc_rtt={:>4}ms grpc={}",
            validator.sample.validator.name,
            validator.sample.latency.as_millis(),
            validator.sample.validator.grpc_url,
        );
    }
    println!("p2p server_name: {}", p2p_server_name);
    println!("system state source: {}", args.system_state_rpc_url);
    println!("subscription source: {}", args.fullnode_grpc_url);
    println!("startup checkpoint floor: {startup_checkpoint_floor}");

    let (observation_tx, observation_rx) = mpsc::channel(OBSERVATION_CHANNEL_CAPACITY);
    let compare_handle = tokio::spawn(run_comparison_logger(observation_rx));
    let subscription_handle = tokio::spawn(run_subscription_path(
        args.fullnode_grpc_url.clone(),
        startup_checkpoint_floor,
        observation_tx.clone(),
    ));
    let fast_handle = tokio::spawn(run_pending_replay_loop(
        fast_validators,
        network,
        state_sync_seeds,
        history_store,
        startup_checkpoint_floor,
        startup_digest,
        observation_tx,
    ));

    tokio::select! {
        result = fast_handle => {
            result.context("pending local replay task panicked")??;
        }
        result = subscription_handle => {
            result.context("subscription task panicked")??;
        }
        result = compare_handle => {
            result.context("comparison task panicked")??;
        }
    }

    Ok(())
}

async fn call_json_rpc<T>(
    client: &reqwest::Client,
    rpc_url: &str,
    method: &str,
    params: serde_json::Value,
) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let response = client
        .post(rpc_url)
        .json(&serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        }))
        .send()
        .await?
        .error_for_status()?;

    let body: JsonRpcResponse<T> = response.json().await?;
    extract_json_rpc_result(body, method)
}

async fn fetch_chain_identifier(client: &reqwest::Client, rpc_url: &str) -> Result<String> {
    call_json_rpc(
        client,
        rpc_url,
        "sui_getChainIdentifier",
        serde_json::json!([]),
    )
    .await
    .context("failed to fetch chain identifier")
}

async fn fetch_startup_checkpoint_baseline(
    fullnode_grpc_url: &str,
) -> Result<(u64, CheckpointDigest)> {
    let mut client = RpcClient::new(fullnode_grpc_url.to_string())
        .with_context(|| format!("failed to build rpc client for {fullnode_grpc_url}"))?;
    let summary = client
        .get_latest_checkpoint()
        .await
        .context("failed to fetch latest certified checkpoint")?;
    Ok((summary.sequence_number, *summary.digest()))
}

fn extract_json_rpc_result<T>(body: JsonRpcResponse<T>, method: &str) -> Result<T> {
    if let Some(err) = body.error {
        return Err(anyhow!(
            "{} returned error {}: {} (data: {:?})",
            method,
            err.code,
            err.message,
            err.data
        ));
    }

    body.result
        .ok_or_else(|| anyhow!("{method} missing `result` field"))
}

async fn fetch_active_validators(
    client: &reqwest::Client,
    rpc_url: &str,
) -> Result<Vec<ValidatorEntry>> {
    let result: CommitteeResult = call_json_rpc(
        client,
        rpc_url,
        "suix_getLatestSuiSystemState",
        serde_json::json!([]),
    )
    .await?;

    if result.active_validators.is_empty() {
        return Err(anyhow!(
            "RPC response contains no validators (expected `activeValidators` or `active_validators`)"
        ));
    }

    Ok(result.active_validators)
}

impl ValidatorEntry {
    fn validator_name(&self) -> &str {
        self.metadata
            .as_ref()
            .map(|m| m.name.as_str())
            .or(self.name.as_deref())
            .unwrap_or("<unknown-validator>")
    }

    fn validator_network_address(&self) -> Option<&str> {
        self.metadata
            .as_ref()
            .map(|m| m.network_address.as_str())
            .or(self.network_address.as_deref())
    }
}

fn build_validator_endpoints(validators: Vec<ValidatorEntry>) -> Result<Vec<ValidatorEndpoint>> {
    let mut endpoints = Vec::new();

    for validator in validators {
        let Some(network_address) = validator.validator_network_address() else {
            continue;
        };

        let (grpc_url, host, port) = match parse_grpc_multiaddr_to_endpoint(network_address) {
            Ok(values) => values,
            Err(_) => continue,
        };

        endpoints.push(ValidatorEndpoint {
            name: validator.validator_name().to_string(),
            grpc_url,
            host,
            port,
        });
    }

    Ok(endpoints)
}

async fn probe_validators(endpoints: Vec<ValidatorEndpoint>) -> Vec<LatencySample> {
    let mut join_set = JoinSet::new();

    for endpoint in endpoints {
        join_set.spawn(async move {
            let tcp_addr = format_host_port(&endpoint.host, endpoint.port);
            let started_at = Instant::now();
            let result = timeout(TCP_PROBE_TIMEOUT, TcpStream::connect(&tcp_addr)).await;
            match result {
                Ok(Ok(_)) => Some(LatencySample {
                    validator: endpoint,
                    latency: started_at.elapsed(),
                }),
                _ => None,
            }
        });
    }

    let mut samples = Vec::new();
    while let Some(join_result) = join_set.join_next().await {
        if let Ok(Some(sample)) = join_result {
            samples.push(sample);
        }
    }

    samples.sort_by_key(|sample| sample.latency);
    samples
}

async fn connect_fast_validators(samples: Vec<LatencySample>) -> Result<Vec<FastValidator>> {
    let mut validators = Vec::new();

    for sample in samples.into_iter().take(FASTEST_VALIDATOR_COUNT) {
        let grpc = match ValidatorGrpcClient::connect(&sample.validator.grpc_url).await {
            Ok(client) => client,
            Err(err) => {
                eprintln!(
                    "skip validator {}: grpc connect failed: {err}",
                    sample.validator.name
                );
                continue;
            }
        };

        validators.push(FastValidator {
            sample,
            grpc,
            stats: Arc::new(RwLock::new(ValidatorRuntimeStats::default())),
        });
    }

    Ok(validators)
}

fn build_p2p_network(server_name: &str) -> Result<AnemoNetwork> {
    let private_key = rand::random::<[u8; 32]>();
    let mut config = anemo::Config::default();
    config.max_frame_size = Some(1 << 30);

    AnemoNetwork::bind("0.0.0.0:0")
        .server_name(server_name)
        .private_key(private_key)
        .config(config)
        .start(anemo::Router::new())
}

fn build_state_sync_seeds() -> Result<Vec<StateSyncSeed>> {
    OFFICIAL_STATE_SYNC_SEEDS
        .iter()
        .map(|(label, address, peer_id_hex)| {
            Ok(StateSyncSeed {
                label: (*label).to_string(),
                peer_id: decode_peer_id_hex(peer_id_hex)?,
                address: parse_p2p_multiaddr_to_anemo(address)?,
                stats: Arc::new(RwLock::new(SeedRuntimeStats::default())),
            })
        })
        .collect()
}

async fn connect_state_sync_seeds(network: &AnemoNetwork, seeds: &[StateSyncSeed]) {
    let mut join_set = JoinSet::new();
    for seed in seeds.iter().cloned() {
        let network = network.clone();
        join_set.spawn(async move {
            let result = ensure_seed_connected(&network, &seed).await;
            (seed, result)
        });
    }

    while let Some(join_result) = join_set.join_next().await {
        match join_result {
            Ok((seed, Ok(()))) => {
                println!("[p2p] connected official seed {}", seed.label);
            }
            Ok((seed, Err(err))) => {
                eprintln!("[p2p] official seed {} connect failed: {err}", seed.label);
            }
            Err(err) => {
                eprintln!("[p2p] seed connect task join error: {err}");
            }
        }
    }
}

async fn run_pending_replay_loop(
    validators: Vec<FastValidator>,
    network: AnemoNetwork,
    seeds: Vec<StateSyncSeed>,
    history_store: Arc<HybridHistoryStore>,
    startup_checkpoint_floor: u64,
    startup_digest: CheckpointDigest,
    observation_tx: mpsc::Sender<ObjectObservation>,
) -> Result<()> {
    let mut next_sequence = startup_checkpoint_floor + 1;
    let mut expected_previous_digest = startup_digest;
    let mut last_seen_latest_pending = startup_checkpoint_floor;
    let mut replay_store =
        RemoteBackedMemoryStore::new(startup_checkpoint_floor, history_store.clone());
    let mut engine_cache = EngineCache::default();

    loop {
        let latest_pending = poll_latest_pending_sequence(&validators).await;
        if latest_pending > last_seen_latest_pending {
            println!(
                "[fast] latest pending checkpoint advanced: {} -> {}",
                last_seen_latest_pending, latest_pending
            );
            last_seen_latest_pending = latest_pending;
        }

        if latest_pending < next_sequence {
            sleep(POLL_INTERVAL).await;
            continue;
        }

        let (summary_validator, discovered_at_ms, summary) =
            fetch_pending_summary_for_sequence(&validators, next_sequence).await?;
        if summary.sequence_number != next_sequence {
            sleep(POLL_INTERVAL).await;
            continue;
        }

        if summary.previous_digest != Some(expected_previous_digest) {
            return Err(anyhow!(
                "pending checkpoint chain discontinuity at sequence {}: expected previous digest {}, got {:?}",
                summary.sequence_number,
                expected_previous_digest,
                summary.previous_digest
            ));
        }

        let (contents, contents_seed) =
            fetch_full_checkpoint_contents(&network, &seeds, &summary).await?;
        contents
            .verify_digests(summary.content_digest)
            .with_context(|| {
                format!(
                    "checkpoint {} digest verification failed",
                    summary.sequence_number
                )
            })?;

        replay_store.set_checkpoint(summary.sequence_number);
        let source_label = format!(
            "summary_validator={} contents_peer={} path=pending_local_replay",
            summary_validator, contents_seed
        );
        let observations = replay_pending_checkpoint(
            &mut replay_store,
            &mut engine_cache,
            history_store.as_ref(),
            &summary,
            &contents,
            discovered_at_ms,
            source_label.clone(),
        )?;

        println!(
            "[fast] checkpoint={} validator={} contents_peer={} observations={}",
            summary.sequence_number,
            summary_validator,
            contents_seed,
            observations.len()
        );

        for observation in observations {
            if let Err(err) = observation_tx.send(observation).await {
                eprintln!("[fast] observation channel closed: {err}");
                return Ok(());
            }
        }

        expected_previous_digest = summary.digest();
        next_sequence += 1;
    }
}

async fn poll_latest_pending_sequence(validators: &[FastValidator]) -> u64 {
    let mut join_set = JoinSet::new();
    for validator in validators.iter().cloned() {
        join_set.spawn(async move {
            let started_at = Instant::now();
            let request = CheckpointRequestV2 {
                sequence_number: None,
                request_content: false,
                certified: false,
            };
            let response = match timeout(
                VALIDATOR_GRPC_TIMEOUT,
                validator.grpc.checkpoint_v2(request),
            )
            .await
            {
                Ok(Ok(response)) => {
                    record_checkpoint_success(&validator.stats, started_at.elapsed());
                    response
                }
                Ok(Err(err)) => {
                    record_checkpoint_failure(&validator.stats);
                    eprintln!(
                        "[fast] validator {} latest checkpoint_v2 failed: {err}",
                        validator.sample.validator.name
                    );
                    return None;
                }
                Err(_) => {
                    record_checkpoint_failure(&validator.stats);
                    eprintln!(
                        "[fast] validator {} latest checkpoint_v2 timeout",
                        validator.sample.validator.name
                    );
                    return None;
                }
            };

            match response.checkpoint {
                Some(CheckpointSummaryResponse::Pending(summary)) => Some(summary.sequence_number),
                _ => None,
            }
        });
    }

    let mut latest = 0u64;
    while let Some(join_result) = join_set.join_next().await {
        if let Ok(Some(sequence)) = join_result {
            latest = latest.max(sequence);
        }
    }

    latest
}

async fn fetch_pending_summary_for_sequence(
    validators: &[FastValidator],
    sequence_number: u64,
) -> Result<(String, u128, CheckpointSummary)> {
    let mut candidates = validators.to_vec();
    candidates.sort_by(compare_checkpoint_validators);

    let mut join_set = JoinSet::new();
    for validator in candidates {
        join_set.spawn(async move {
            let request = CheckpointRequestV2 {
                sequence_number: Some(sequence_number),
                request_content: false,
                certified: false,
            };
            let started_at = Instant::now();
            let discovered_at_ms = unix_time_ms();
            let result = match timeout(
                VALIDATOR_GRPC_TIMEOUT,
                validator.grpc.checkpoint_v2(request),
            )
            .await
            {
                Ok(Ok(response)) => {
                    record_checkpoint_success(&validator.stats, started_at.elapsed());
                    response
                }
                Ok(Err(err)) => {
                    record_checkpoint_failure(&validator.stats);
                    return Err(anyhow!(
                        "{}: checkpoint_v2 failed for sequence {}: {err}",
                        validator.sample.validator.name,
                        sequence_number
                    ));
                }
                Err(_) => {
                    record_checkpoint_failure(&validator.stats);
                    return Err(anyhow!(
                        "{}: checkpoint_v2 timeout for sequence {}",
                        validator.sample.validator.name,
                        sequence_number
                    ));
                }
            };

            let summary = match result.checkpoint {
                Some(CheckpointSummaryResponse::Pending(summary))
                    if summary.sequence_number == sequence_number =>
                {
                    summary
                }
                _ => return Err(anyhow!("pending summary not available")),
            };

            Ok((
                validator.sample.validator.name.clone(),
                discovered_at_ms,
                summary,
            ))
        });
    }

    let mut last_error = None;
    while let Some(join_result) = join_set.join_next().await {
        match join_result {
            Ok(Ok(result)) => {
                join_set.abort_all();
                return Ok(result);
            }
            Ok(Err(err)) => last_error = Some(err),
            Err(err) => last_error = Some(anyhow!("checkpoint summary join error: {err}")),
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow!("no validator returned pending summary")))
}

async fn fetch_full_checkpoint_contents(
    network: &AnemoNetwork,
    seeds: &[StateSyncSeed],
    summary: &CheckpointSummary,
) -> Result<(VersionedFullCheckpointContents, String)> {
    let mut candidates = seeds.to_vec();
    candidates.sort_by(compare_state_sync_seeds);
    candidates.truncate(CONTENTS_RACE_WIDTH);

    let digest = summary.content_digest;
    let mut join_set = JoinSet::new();
    for seed in candidates {
        let network = network.clone();
        join_set.spawn(async move {
            ensure_seed_connected(&network, &seed).await?;
            let peer = network
                .peer(seed.peer_id)
                .ok_or_else(|| anyhow!("missing connected p2p peer for {}", seed.label))?;

            let started_at = Instant::now();
            let mut client = StateSyncClient::new(peer);
            let request = AnemoRequest::new(digest).with_timeout(STATE_SYNC_TIMEOUT);
            let response = client
                .get_checkpoint_contents_v2(request)
                .await
                .map_err(|status| {
                    anyhow!(
                        "GetCheckpointContentsV2 failed on {}: {status:?}",
                        seed.label
                    )
                })?;

            let contents = response.into_inner().ok_or_else(|| {
                anyhow!(
                    "GetCheckpointContentsV2 returned no contents on {}",
                    seed.label
                )
            })?;

            record_seed_success(&seed, started_at.elapsed());
            Ok::<_, anyhow::Error>((seed.label.clone(), contents))
        });
    }

    let mut last_error = None;
    while let Some(join_result) = join_set.join_next().await {
        match join_result {
            Ok(Ok(result)) => {
                join_set.abort_all();
                return Ok((result.1, result.0));
            }
            Ok(Err(err)) => last_error = Some(err),
            Err(err) => last_error = Some(anyhow!("state sync contents join error: {err}")),
        }
    }

    for seed in seeds {
        record_seed_failure(seed);
    }

    Err(last_error.unwrap_or_else(|| anyhow!("state sync contents unavailable")))
}

fn replay_pending_checkpoint(
    replay_store: &mut RemoteBackedMemoryStore,
    engine_cache: &mut EngineCache,
    history_store: &HybridHistoryStore,
    summary: &CheckpointSummary,
    contents: &VersionedFullCheckpointContents,
    path_discovered_at_ms: u128,
    source_label: String,
) -> Result<Vec<ObjectObservation>> {
    let engine = engine_cache.get_or_create(summary.epoch, history_store)?;
    let started_at = Instant::now();
    let mut observations = Vec::new();

    for execution_data in contents.iter() {
        hydrate_transaction_inputs(
            replay_store,
            history_store,
            execution_data.transaction.transaction_data(),
            &execution_data.effects,
            summary.sequence_number,
        )?;

        let output = engine.replay_transaction(
            replay_store,
            execution_data.transaction.transaction_data(),
            &execution_data.effects,
            *execution_data.transaction.digest(),
        )?;

        let seen_at_ms = unix_time_ms();
        for object in output.written_objects.values() {
            if let Some(observation) = build_observation_from_object(
                ObservationSource::PendingLocalReplay,
                summary.sequence_number,
                path_discovered_at_ms,
                seen_at_ms,
                object,
                source_label.clone(),
            )? {
                observations.push(observation);
            }
        }
    }

    println!(
        "[fast] replayed checkpoint={} epoch={} txs={} replay_ms={}",
        summary.sequence_number,
        summary.epoch,
        contents.iter().count(),
        started_at.elapsed().as_millis(),
    );

    Ok(observations)
}

fn hydrate_transaction_inputs(
    replay_store: &mut RemoteBackedMemoryStore,
    history_store: &dyn HistoryObjectStore,
    transaction_data: &TransactionData,
    effects: &sui_types::effects::TransactionEffects,
    checkpoint_sequence: u64,
) -> Result<()> {
    let keys = prefetch_requests_for_transaction(transaction_data, effects)?
        .into_iter()
        .map(|request| match request {
            PrefetchRequest::ObjectVersion { object_id, version } => HistoryObjectKey {
                object_id,
                version_query: VersionQuery::Version(version),
            },
            PrefetchRequest::Package { package_id } => HistoryObjectKey {
                object_id: package_id,
                version_query: VersionQuery::AtCheckpoint(checkpoint_sequence),
            },
        })
        .collect::<Vec<_>>();

    if keys.is_empty() {
        return Ok(());
    }

    let objects = history_store.get_objects(&keys)?;
    replay_store.insert_objects(objects.into_iter().flatten().map(|(object, _)| object));
    Ok(())
}

async fn run_subscription_path(
    fullnode_grpc_url: String,
    startup_checkpoint_floor: u64,
    observation_tx: mpsc::Sender<ObjectObservation>,
) -> Result<()> {
    let read_mask = FieldMask::from_paths([
        rpc::Checkpoint::path_builder().sequence_number(),
        rpc::Checkpoint::path_builder()
            .transactions()
            .effects()
            .bcs()
            .value(),
        rpc::Checkpoint::path_builder()
            .objects()
            .objects()
            .bcs()
            .value(),
    ]);

    loop {
        let mut client = SubscriptionServiceClient::connect(fullnode_grpc_url.clone())
            .await
            .with_context(|| {
                format!("failed to connect subscription client to {fullnode_grpc_url}")
            })?
            .max_decoding_message_size(MAX_SUBSCRIPTION_MESSAGE_SIZE);

        let mut stream = client
            .subscribe_checkpoints(
                SubscribeCheckpointsRequest::default().with_read_mask(read_mask.clone()),
            )
            .await
            .context("SubscribeCheckpoints failed")?
            .into_inner();

        while let Some(item) = stream.next().await {
            let response = match item {
                Ok(response) => response,
                Err(err) => {
                    eprintln!("[subscription] stream error: {err}");
                    break;
                }
            };

            let checkpoint_received_at_ms = unix_time_ms();
            let Some(checkpoint) = response.checkpoint else {
                continue;
            };
            let checkpoint_sequence = response
                .cursor
                .or(checkpoint.sequence_number)
                .unwrap_or_default();
            if checkpoint_sequence <= startup_checkpoint_floor {
                continue;
            }

            match extract_subscription_observations(
                checkpoint_sequence,
                checkpoint_received_at_ms,
                checkpoint,
                "fullnode subscription".to_string(),
            ) {
                Ok(observations) => {
                    for observation in observations {
                        println!(
                            "[subscription] checkpoint={} object={} version={} type={} seen_at={}",
                            observation.checkpoint_sequence,
                            observation.object_key.object_id,
                            observation.object_key.version,
                            observation.object_type,
                            observation.seen_at_ms
                        );

                        if let Err(err) = observation_tx.send(observation).await {
                            eprintln!("[subscription] observation channel closed: {err}");
                            return Ok(());
                        }
                    }
                }
                Err(err) => eprintln!(
                    "[subscription] checkpoint={} failed to extract target objects: {err}",
                    checkpoint_sequence
                ),
            }
        }

        sleep(SUBSCRIPTION_RETRY_DELAY).await;
    }
}

fn extract_subscription_observations(
    checkpoint_sequence: u64,
    checkpoint_received_at_ms: u128,
    checkpoint: rpc::Checkpoint,
    source_label: String,
) -> Result<Vec<ObjectObservation>> {
    let rpc::Checkpoint {
        transactions,
        objects,
        ..
    } = checkpoint;
    let mut output_keys = HashSet::new();
    for transaction in transactions {
        let Some(effects_bcs) = transaction.effects.and_then(|effects| effects.bcs) else {
            continue;
        };
        let effects: TransactionEffects = effects_bcs.deserialize().with_context(|| {
            format!(
                "failed to deserialize transaction effects for checkpoint {checkpoint_sequence}"
            )
        })?;

        for change in effects.object_changes() {
            let Some(version) = change.output_version else {
                continue;
            };
            output_keys.insert(ObjectVersionKey {
                object_id: change.id,
                version: version.value(),
            });
        }
    }

    let mut observations = Vec::new();
    let Some(object_set) = objects else {
        return Ok(observations);
    };

    for object_message in object_set.objects {
        let Some(object_bcs) = object_message.bcs else {
            continue;
        };
        let object: SuiObject = object_bcs.deserialize().with_context(|| {
            format!("failed to deserialize object bcs for checkpoint {checkpoint_sequence}")
        })?;
        let key = ObjectVersionKey {
            object_id: object.id(),
            version: object.version().value(),
        };

        if !output_keys.contains(&key) {
            continue;
        }

        if let Some(observation) = build_observation_from_object(
            ObservationSource::CheckpointSubscription,
            checkpoint_sequence,
            checkpoint_received_at_ms,
            checkpoint_received_at_ms,
            &object,
            source_label.clone(),
        )? {
            observations.push(observation);
        }
    }

    Ok(observations)
}

fn build_observation_from_object(
    source: ObservationSource,
    checkpoint_sequence: u64,
    path_discovered_at_ms: u128,
    seen_at_ms: u128,
    object: &SuiObject,
    source_label: String,
) -> Result<Option<ObjectObservation>> {
    let Some(object_type) = object.struct_tag().map(|tag| tag.to_canonical_string(true)) else {
        return Ok(None);
    };

    if !is_target_pool_object_type(&object_type) {
        return Ok(None);
    }

    let object_bcs = bcs::to_bytes(object)
        .with_context(|| format!("failed to serialize object {} for logging", object.id()))?;
    let move_contents_len = object.data.try_as_move().map(|m| m.contents().len());

    Ok(Some(ObjectObservation {
        source,
        checkpoint_sequence,
        seen_at_ms,
        path_discovered_at_ms,
        object_key: ObjectVersionKey {
            object_id: object.id(),
            version: object.version().value(),
        },
        object_digest: object.digest().to_string(),
        object_type,
        owner: format!("{:?}", object.owner()),
        object_bcs_len: object_bcs.len(),
        move_contents_len,
        source_label,
    }))
}

fn is_target_pool_object_type(object_type: &str) -> bool {
    object_type.starts_with(TARGET_POOL_OBJECT_TYPE_PREFIX)
}

async fn run_comparison_logger(
    mut observation_rx: mpsc::Receiver<ObjectObservation>,
) -> Result<()> {
    let mut pairs = HashMap::<ObjectVersionKey, ObservationPair>::new();

    while let Some(observation) = observation_rx.recv().await {
        let pair = pairs.entry(observation.object_key).or_default();
        match observation.source {
            ObservationSource::PendingLocalReplay => pair.fast = Some(observation.clone()),
            ObservationSource::CheckpointSubscription => {
                pair.subscription = Some(observation.clone())
            }
        }

        let mut remove_key = None;
        if let (Some(fast), Some(subscription)) = (&pair.fast, &pair.subscription) {
            let delta_ms = subscription.seen_at_ms as i128 - fast.seen_at_ms as i128;
            println!(
                "[compare] object={} version={} type={} fast_at={} sub_at={} delta_ms={} fast_checkpoint={} sub_checkpoint={}",
                fast.object_key.object_id,
                fast.object_key.version,
                fast.object_type,
                fast.seen_at_ms,
                subscription.seen_at_ms,
                delta_ms,
                fast.checkpoint_sequence,
                subscription.checkpoint_sequence,
            );
            println!(
                "          fast_source={} fast_path_at={} fast_owner={} fast_digest={} fast_object_bcs_len={} fast_move_contents_len={:?}",
                fast.source_label,
                fast.path_discovered_at_ms,
                fast.owner,
                fast.object_digest,
                fast.object_bcs_len,
                fast.move_contents_len,
            );
            println!(
                "          sub_source={} sub_path_at={} sub_owner={} sub_digest={} sub_object_bcs_len={} sub_move_contents_len={:?}",
                subscription.source_label,
                subscription.path_discovered_at_ms,
                subscription.owner,
                subscription.object_digest,
                subscription.object_bcs_len,
                subscription.move_contents_len,
            );
            remove_key = Some(fast.object_key);
        }

        if let Some(key) = remove_key {
            pairs.remove(&key);
        }
    }

    Ok(())
}

fn compare_checkpoint_validators(left: &FastValidator, right: &FastValidator) -> Ordering {
    let left_stats = left.stats.read().unwrap();
    let right_stats = right.stats.read().unwrap();

    effective_checkpoint_rtt_ms(left, &left_stats)
        .partial_cmp(&effective_checkpoint_rtt_ms(right, &right_stats))
        .unwrap_or(Ordering::Equal)
        .then_with(|| {
            right_stats
                .checkpoint_successes
                .cmp(&left_stats.checkpoint_successes)
        })
        .then_with(|| {
            left_stats
                .checkpoint_failures
                .cmp(&right_stats.checkpoint_failures)
        })
        .then_with(|| left.sample.validator.name.cmp(&right.sample.validator.name))
}

fn effective_checkpoint_rtt_ms(validator: &FastValidator, stats: &ValidatorRuntimeStats) -> f64 {
    stats
        .avg_checkpoint_rtt_ms
        .unwrap_or(validator.sample.latency.as_secs_f64() * 1000.0)
}

fn record_checkpoint_success(stats: &Arc<RwLock<ValidatorRuntimeStats>>, rtt: Duration) {
    let mut guard = stats.write().unwrap();
    let sample = rtt.as_secs_f64() * 1000.0;
    guard.avg_checkpoint_rtt_ms = Some(match guard.avg_checkpoint_rtt_ms {
        Some(previous) => (previous * 0.7) + (sample * 0.3),
        None => sample,
    });
    guard.checkpoint_successes += 1;
}

fn record_checkpoint_failure(stats: &Arc<RwLock<ValidatorRuntimeStats>>) {
    let mut guard = stats.write().unwrap();
    guard.checkpoint_failures += 1;
}

fn compare_state_sync_seeds(left: &StateSyncSeed, right: &StateSyncSeed) -> Ordering {
    let left_stats = left.stats.read().unwrap();
    let right_stats = right.stats.read().unwrap();

    left_stats
        .avg_contents_rtt_ms
        .partial_cmp(&right_stats.avg_contents_rtt_ms)
        .unwrap_or(Ordering::Equal)
        .then_with(|| {
            right_stats
                .contents_successes
                .cmp(&left_stats.contents_successes)
        })
        .then_with(|| {
            left_stats
                .contents_failures
                .cmp(&right_stats.contents_failures)
        })
        .then_with(|| left.label.cmp(&right.label))
}

fn record_seed_success(seed: &StateSyncSeed, rtt: Duration) {
    let mut guard = seed.stats.write().unwrap();
    let sample = rtt.as_secs_f64() * 1000.0;
    guard.avg_contents_rtt_ms = Some(match guard.avg_contents_rtt_ms {
        Some(previous) => (previous * 0.7) + (sample * 0.3),
        None => sample,
    });
    guard.contents_successes += 1;
}

fn record_seed_failure(seed: &StateSyncSeed) {
    let mut guard = seed.stats.write().unwrap();
    guard.contents_failures += 1;
}

async fn ensure_seed_connected(network: &AnemoNetwork, seed: &StateSyncSeed) -> Result<()> {
    if network.peer(seed.peer_id).is_some() {
        return Ok(());
    }

    let connected_peer_id = timeout(
        P2P_CONNECT_TIMEOUT,
        network.connect_with_peer_id(seed.address.clone(), seed.peer_id),
    )
    .await
    .map_err(|_| anyhow!("p2p connect timeout for {}", seed.label))??;

    if connected_peer_id != seed.peer_id {
        return Err(anyhow!(
            "p2p peer id mismatch for {}: expected {}, got {}",
            seed.label,
            seed.peer_id,
            connected_peer_id
        ));
    }

    Ok(())
}

fn decode_peer_id_hex(hex: &str) -> Result<PeerId> {
    if hex.len() != 64 {
        return Err(anyhow!("peer id hex length is {}, expected 64", hex.len()));
    }

    let mut bytes = [0u8; 32];
    for (index, chunk) in hex.as_bytes().chunks(2).enumerate() {
        let text = std::str::from_utf8(chunk)?;
        bytes[index] = u8::from_str_radix(text, 16)
            .with_context(|| format!("invalid hex peer id byte `{text}`"))?;
    }

    Ok(PeerId(bytes))
}

fn unix_time_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

fn format_host_port(host: &str, port: u16) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

fn parse_grpc_multiaddr_to_endpoint(addr: &str) -> Result<(String, String, u16)> {
    let ma = Multiaddr::from_str(addr).with_context(|| format!("invalid multiaddr `{addr}`"))?;

    let mut host = None;
    let mut port = None;
    let mut scheme = "http";

    for protocol in ma.iter() {
        match protocol {
            Protocol::Dns(name)
            | Protocol::Dns4(name)
            | Protocol::Dns6(name)
            | Protocol::Dnsaddr(name) => host = Some(name.to_string()),
            Protocol::Ip4(ip) => host = Some(ip.to_string()),
            Protocol::Ip6(ip) => host = Some(ip.to_string()),
            Protocol::Tcp(p) => port = Some(p),
            Protocol::Http => scheme = "http",
            Protocol::Https => scheme = "https",
            _ => {}
        }
    }

    let host = host.ok_or_else(|| anyhow!("No host in multiaddr"))?;
    let port = port.ok_or_else(|| anyhow!("No tcp port in multiaddr"))?;
    let endpoint = format!("{scheme}://{}", format_host_port(&host, port));
    Ok((endpoint, host, port))
}

fn parse_p2p_multiaddr_to_anemo(addr: &str) -> Result<anemo::types::Address> {
    let ma =
        Multiaddr::from_str(addr).with_context(|| format!("invalid p2p multiaddr `{addr}`"))?;

    let mut iter = ma.iter();
    match (iter.next(), iter.next()) {
        (Some(Protocol::Ip4(ip)), Some(Protocol::Udp(port))) => Ok((ip, port).into()),
        (Some(Protocol::Ip6(ip)), Some(Protocol::Udp(port))) => Ok((ip, port).into()),
        (Some(Protocol::Dns(host)), Some(Protocol::Udp(port)))
        | (Some(Protocol::Dns4(host)), Some(Protocol::Udp(port)))
        | (Some(Protocol::Dns6(host)), Some(Protocol::Udp(port)))
        | (Some(Protocol::Dnsaddr(host)), Some(Protocol::Udp(port))) => {
            Ok((host.to_string(), port).into())
        }
        _ => Err(anyhow!("unsupported p2p multiaddr `{addr}`")),
    }
}

fn block_on_runtime<F, T>(future: F) -> T
where
    F: Future<Output = T> + Send,
    T: Send,
{
    if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("failed to build Tokio runtime");
                    rt.block_on(future)
                })
                .join()
                .expect("failed to join scoped thread running nested runtime")
        })
    } else {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to build Tokio runtime");
        rt.block_on(future)
    }
}
