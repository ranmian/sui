// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use std::fs;
use std::path::PathBuf;
use sui_replay_2::local_replay::{LocalReplayEngine, VersionedMemoryStore};
use sui_types::message_envelope::Message;
use sui_types::messages_checkpoint::VersionedFullCheckpointContents;
use sui_types::object::Object;
use sui_types::supported_protocol_versions::{Chain, ProtocolConfig, ProtocolVersion};

#[derive(Debug, Parser)]
#[command(name = "local-checkpoint-replay")]
#[command(about = "Replay checkpoint contents with a pure in-memory store")]
struct Args {
    #[arg(long)]
    contents_bcs: PathBuf,

    #[arg(long)]
    objects_bcs: PathBuf,

    #[arg(long, default_value = "mainnet")]
    chain: Chain,

    #[arg(long)]
    protocol_version: u64,

    #[arg(long)]
    epoch_id: u64,

    #[arg(long)]
    epoch_start_timestamp_ms: u64,

    #[arg(long)]
    reference_gas_price: u64,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let protocol_config = ProtocolConfig::get_for_version_if_supported(
        ProtocolVersion::new(args.protocol_version),
        args.chain,
    )
    .ok_or_else(|| {
        anyhow!(
            "unsupported protocol version {} for chain {}",
            args.protocol_version,
            args.chain.as_str()
        )
    })?;

    let contents_bytes = fs::read(&args.contents_bcs)
        .with_context(|| format!("failed to read {}", args.contents_bcs.display()))?;
    let contents: VersionedFullCheckpointContents = bcs::from_bytes(&contents_bytes)
        .context("failed to deserialize checkpoint contents bcs")?;

    let objects_bytes = fs::read(&args.objects_bcs)
        .with_context(|| format!("failed to read {}", args.objects_bcs.display()))?;
    let objects: Vec<Object> =
        bcs::from_bytes(&objects_bytes).context("failed to deserialize object preload bcs")?;

    let mut store = VersionedMemoryStore::new();
    store.insert_objects(objects);

    let engine = LocalReplayEngine::new(
        protocol_config,
        args.epoch_id,
        args.epoch_start_timestamp_ms,
        args.reference_gas_price,
    )?;

    let outputs = engine.replay_checkpoint_contents(&mut store, &contents)?;
    println!("replayed_transactions={}", outputs.len());

    for output in outputs {
        println!(
            "tx={} status_ok={} writes={} effects_digest={}",
            output.tx_digest,
            output.result.is_ok(),
            output.written_objects.len(),
            output.effects.digest()
        );
    }

    Ok(())
}
