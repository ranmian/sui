// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result, anyhow};
use move_core_types::language_storage::TypeTag;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::{Arc, RwLock};
use sui_data_store::{
    ObjectKey as HistoryObjectKey, ObjectStore as HistoryObjectStore, VersionQuery,
};
use sui_execution::Executor;
use sui_types::base_types::{ObjectID, ObjectRef, SequenceNumber, VersionNumber};
use sui_types::committee::EpochId;
use sui_types::digests::TransactionDigest;
use sui_types::effects::{
    InputConsensusObject, TransactionEffects, TransactionEffectsAPI, UnchangedConsensusKind,
};
use sui_types::error::{ExecutionError, SuiErrorKind, SuiResult};
use sui_types::execution_params::{
    ExecutionOrEarlyError, FundsWithdrawStatus, get_early_execution_error,
};
use sui_types::gas::SuiGasStatus;
use sui_types::inner_temporary_store::InnerTemporaryStore;
use sui_types::message_envelope::Message;
use sui_types::messages_checkpoint::VersionedFullCheckpointContents;
use sui_types::metrics::LimitsMetrics;
use sui_types::object::{Object, Owner};
use sui_types::storage::{
    BackingPackageStore, ChildObjectResolver, ObjectStore, PackageObject, ParentSync,
};
use sui_types::supported_protocol_versions::ProtocolConfig;
use sui_types::transaction::{
    CallArg, CheckedInputObjects, Command, InputObjectKind, InputObjects, ObjectArg,
    ObjectReadResult, TransactionData, TransactionDataAPI, TransactionKind,
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum PrefetchRequest {
    ObjectVersion { object_id: ObjectID, version: u64 },
    Package { package_id: ObjectID },
}

#[derive(Debug)]
pub struct ReplayTransactionOutput {
    pub tx_digest: TransactionDigest,
    pub effects: TransactionEffects,
    pub written_objects: BTreeMap<ObjectID, Object>,
    pub result: Result<(), ExecutionError>,
}

pub trait ReplayStateStore:
    BackingPackageStore + ChildObjectResolver + ObjectStore + ParentSync
{
    fn fetch_object_at_version(
        &self,
        object_id: &ObjectID,
        version: SequenceNumber,
    ) -> Result<Option<Object>>;

    fn insert_object(&mut self, object: Object);

    fn insert_objects<I>(&mut self, objects: I)
    where
        Self: Sized,
        I: IntoIterator<Item = Object>,
    {
        for object in objects {
            self.insert_object(object);
        }
    }

    fn apply_execution_output(
        &mut self,
        inner_store: &InnerTemporaryStore,
        effects: &TransactionEffects,
    );
}

pub struct LocalReplayEngine {
    executor: Arc<dyn Executor + Send + Sync>,
    protocol_config: ProtocolConfig,
    metrics: Arc<LimitsMetrics>,
    epoch_id: EpochId,
    epoch_start_timestamp_ms: u64,
    reference_gas_price: u64,
}

#[derive(Debug, Default, Clone)]
pub struct VersionedMemoryStore {
    objects: BTreeMap<ObjectID, BTreeMap<u64, Object>>,
    latest_live_versions: BTreeMap<ObjectID, u64>,
}

pub struct RemoteBackedMemoryStore {
    overlay: RwLock<VersionedMemoryStore>,
    checkpoint: u64,
    remote_store: Arc<dyn HistoryObjectStore + Send + Sync>,
}

impl LocalReplayEngine {
    pub fn new(
        protocol_config: ProtocolConfig,
        epoch_id: EpochId,
        epoch_start_timestamp_ms: u64,
        reference_gas_price: u64,
    ) -> Result<Self> {
        let executor = sui_execution::executor(&protocol_config, true)
            .context("failed to construct sui executor")?;
        let registry = prometheus::Registry::new();
        let metrics = Arc::new(LimitsMetrics::new(&registry));

        Ok(Self {
            executor,
            protocol_config,
            metrics,
            epoch_id,
            epoch_start_timestamp_ms,
            reference_gas_price,
        })
    }

    pub fn replay_checkpoint_contents<S: ReplayStateStore>(
        &self,
        store: &mut S,
        contents: &VersionedFullCheckpointContents,
    ) -> Result<Vec<ReplayTransactionOutput>> {
        let mut outputs = Vec::new();
        for execution_data in contents.iter() {
            outputs.push(self.replay_transaction(
                store,
                execution_data.transaction.transaction_data(),
                &execution_data.effects,
                *execution_data.transaction.digest(),
            )?);
        }
        Ok(outputs)
    }

    pub fn replay_transaction<S: ReplayStateStore>(
        &self,
        store: &mut S,
        transaction_data: &TransactionData,
        expected_effects: &TransactionEffects,
        tx_digest: TransactionDigest,
    ) -> Result<ReplayTransactionOutput> {
        let input_objects = build_checked_input_objects(store, transaction_data, expected_effects)
            .with_context(|| {
                format!("failed to build replay inputs for transaction {tx_digest}")
            })?;

        let gas_status = if transaction_data.kind().is_system_tx() {
            SuiGasStatus::new_unmetered()
        } else {
            SuiGasStatus::new(
                transaction_data.gas_data().budget,
                transaction_data.gas_data().price,
                self.reference_gas_price,
                &self.protocol_config,
            )
            .context("failed to construct gas status")?
        };

        let early_execution_error = get_early_execution_error(
            &tx_digest,
            &input_objects,
            &HashSet::<TransactionDigest>::new(),
            &FundsWithdrawStatus::MaybeSufficient,
        );
        let execution_params = match early_execution_error {
            Some(error) => ExecutionOrEarlyError::Err(error),
            None => ExecutionOrEarlyError::Ok(()),
        };

        let (inner_store, _gas_status, actual_effects, _timings, result) =
            self.executor.execute_transaction_to_effects(
                store,
                &self.protocol_config,
                self.metrics.clone(),
                false,
                execution_params,
                &self.epoch_id,
                self.epoch_start_timestamp_ms,
                input_objects,
                transaction_data.gas_data().clone(),
                gas_status,
                transaction_data.kind().clone(),
                transaction_data.sender(),
                tx_digest,
                &mut None,
            );

        if actual_effects.digest() != expected_effects.digest() {
            return Err(anyhow!(
                "effects digest mismatch for transaction {tx_digest}: expected {}, got {}",
                expected_effects.digest(),
                actual_effects.digest()
            ));
        }

        store.apply_execution_output(&inner_store, &actual_effects);

        Ok(ReplayTransactionOutput {
            tx_digest,
            effects: actual_effects,
            written_objects: inner_store.written.clone(),
            result,
        })
    }
}

impl VersionedMemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert_object(&mut self, object: Object) {
        let object_id = object.id();
        let version = object.version().value();
        self.objects
            .entry(object_id)
            .or_default()
            .insert(version, object);
        self.latest_live_versions
            .entry(object_id)
            .and_modify(|current| {
                if version > *current {
                    *current = version;
                }
            })
            .or_insert(version);
    }

    pub fn insert_objects<I>(&mut self, objects: I)
    where
        I: IntoIterator<Item = Object>,
    {
        for object in objects {
            self.insert_object(object);
        }
    }

    pub fn get_object_at_version(
        &self,
        object_id: &ObjectID,
        version: SequenceNumber,
    ) -> Option<Object> {
        self.objects
            .get(object_id)
            .and_then(|versions| versions.get(&version.value()))
            .cloned()
    }

    pub fn get_object_at_most_version(
        &self,
        object_id: &ObjectID,
        version_upper_bound: SequenceNumber,
    ) -> Option<Object> {
        self.objects.get(object_id).and_then(|versions| {
            versions
                .range(..=version_upper_bound.value())
                .next_back()
                .map(|(_, object)| object.clone())
        })
    }

    pub fn latest_live_version(&self, object_id: &ObjectID) -> Option<u64> {
        self.latest_live_versions.get(object_id).copied()
    }

    pub fn apply_execution_output(
        &mut self,
        inner_store: &InnerTemporaryStore,
        effects: &TransactionEffects,
    ) {
        for object in inner_store.written.values().cloned() {
            self.insert_object(object);
        }

        for (object_id, _version, _digest) in effects.deleted() {
            self.latest_live_versions.remove(&object_id);
        }
        for (object_id, _version, _digest) in effects.unwrapped_then_deleted() {
            self.latest_live_versions.remove(&object_id);
        }
        for (object_id, _version, _digest) in effects.wrapped() {
            self.latest_live_versions.remove(&object_id);
        }
    }
}

impl RemoteBackedMemoryStore {
    pub fn new(checkpoint: u64, remote_store: Arc<dyn HistoryObjectStore + Send + Sync>) -> Self {
        Self {
            overlay: RwLock::new(VersionedMemoryStore::new()),
            checkpoint,
            remote_store,
        }
    }

    pub fn set_checkpoint(&mut self, checkpoint: u64) {
        self.checkpoint = checkpoint;
    }

    fn fetch_remote_object(&self, key: HistoryObjectKey) -> Result<Option<Object>> {
        let object = self
            .remote_store
            .get_objects(&[key])?
            .into_iter()
            .next()
            .flatten()
            .map(|(object, _actual_version)| object);

        if let Some(object) = &object {
            self.overlay.write().unwrap().insert_object(object.clone());
        }

        Ok(object)
    }

    fn get_object_at_checkpoint(&self, object_id: &ObjectID) -> Result<Option<Object>> {
        if let Some(object) = self.overlay.read().unwrap().get_object(object_id) {
            return Ok(Some(object));
        }

        self.fetch_remote_object(HistoryObjectKey {
            object_id: *object_id,
            version_query: VersionQuery::AtCheckpoint(self.checkpoint),
        })
    }

    fn get_object_at_most_version(
        &self,
        object_id: &ObjectID,
        version_upper_bound: SequenceNumber,
    ) -> Result<Option<Object>> {
        if let Some(object) = self
            .overlay
            .read()
            .unwrap()
            .get_object_at_most_version(object_id, version_upper_bound)
        {
            return Ok(Some(object));
        }

        self.fetch_remote_object(HistoryObjectKey {
            object_id: *object_id,
            version_query: VersionQuery::RootVersion(version_upper_bound.value()),
        })
    }
}

impl ReplayStateStore for VersionedMemoryStore {
    fn fetch_object_at_version(
        &self,
        object_id: &ObjectID,
        version: SequenceNumber,
    ) -> Result<Option<Object>> {
        Ok(self.get_object_at_version(object_id, version))
    }

    fn insert_object(&mut self, object: Object) {
        VersionedMemoryStore::insert_object(self, object);
    }

    fn apply_execution_output(
        &mut self,
        inner_store: &InnerTemporaryStore,
        effects: &TransactionEffects,
    ) {
        VersionedMemoryStore::apply_execution_output(self, inner_store, effects);
    }
}

impl ReplayStateStore for RemoteBackedMemoryStore {
    fn fetch_object_at_version(
        &self,
        object_id: &ObjectID,
        version: SequenceNumber,
    ) -> Result<Option<Object>> {
        if let Some(object) = self
            .overlay
            .read()
            .unwrap()
            .get_object_at_version(object_id, version)
        {
            return Ok(Some(object));
        }

        self.fetch_remote_object(HistoryObjectKey {
            object_id: *object_id,
            version_query: VersionQuery::Version(version.value()),
        })
    }

    fn insert_object(&mut self, object: Object) {
        self.overlay.write().unwrap().insert_object(object);
    }

    fn apply_execution_output(
        &mut self,
        inner_store: &InnerTemporaryStore,
        effects: &TransactionEffects,
    ) {
        self.overlay
            .write()
            .unwrap()
            .apply_execution_output(inner_store, effects);
    }
}

impl BackingPackageStore for VersionedMemoryStore {
    fn get_package_object(&self, package_id: &ObjectID) -> SuiResult<Option<PackageObject>> {
        Ok(self.get_object(package_id).map(PackageObject::new))
    }
}

impl BackingPackageStore for RemoteBackedMemoryStore {
    fn get_package_object(&self, package_id: &ObjectID) -> SuiResult<Option<PackageObject>> {
        Ok(self
            .get_object_at_checkpoint(package_id)
            .map_err(|e| SuiErrorKind::Storage(e.to_string()))?
            .map(PackageObject::new))
    }
}

impl ChildObjectResolver for VersionedMemoryStore {
    fn read_child_object(
        &self,
        parent: &ObjectID,
        child: &ObjectID,
        child_version_upper_bound: SequenceNumber,
    ) -> SuiResult<Option<Object>> {
        let object = self.get_object_at_most_version(child, child_version_upper_bound);
        validate_child_owner(parent, child, object)
    }

    fn get_object_received_at_version(
        &self,
        _owner: &ObjectID,
        receiving_object_id: &ObjectID,
        receive_object_at_version: SequenceNumber,
        _epoch_id: EpochId,
    ) -> SuiResult<Option<Object>> {
        Ok(self.get_object_at_version(receiving_object_id, receive_object_at_version))
    }
}

impl ChildObjectResolver for RemoteBackedMemoryStore {
    fn read_child_object(
        &self,
        parent: &ObjectID,
        child: &ObjectID,
        child_version_upper_bound: SequenceNumber,
    ) -> SuiResult<Option<Object>> {
        let object = self
            .get_object_at_most_version(child, child_version_upper_bound)
            .map_err(|e| SuiErrorKind::Storage(e.to_string()))?;
        validate_child_owner(parent, child, object)
    }

    fn get_object_received_at_version(
        &self,
        _owner: &ObjectID,
        receiving_object_id: &ObjectID,
        receive_object_at_version: SequenceNumber,
        _epoch_id: EpochId,
    ) -> SuiResult<Option<Object>> {
        Ok(self
            .fetch_object_at_version(receiving_object_id, receive_object_at_version)
            .map_err(|e| SuiErrorKind::Storage(e.to_string()))?)
    }
}

impl ParentSync for VersionedMemoryStore {
    fn get_latest_parent_entry_ref_deprecated(&self, _object_id: ObjectID) -> Option<ObjectRef> {
        unreachable!("deprecated ParentSync is not expected in local replay");
    }
}

impl ParentSync for RemoteBackedMemoryStore {
    fn get_latest_parent_entry_ref_deprecated(&self, _object_id: ObjectID) -> Option<ObjectRef> {
        unreachable!("deprecated ParentSync is not expected in local replay");
    }
}

impl ObjectStore for VersionedMemoryStore {
    fn get_object(&self, object_id: &ObjectID) -> Option<Object> {
        let version = self.latest_live_versions.get(object_id)?;
        self.objects
            .get(object_id)
            .and_then(|versions| versions.get(version))
            .cloned()
    }

    fn get_object_by_key(&self, object_id: &ObjectID, version: VersionNumber) -> Option<Object> {
        self.get_object_at_version(object_id, version)
    }
}

impl ObjectStore for RemoteBackedMemoryStore {
    fn get_object(&self, object_id: &ObjectID) -> Option<Object> {
        self.get_object_at_checkpoint(object_id).ok().flatten()
    }

    fn get_object_by_key(&self, object_id: &ObjectID, version: VersionNumber) -> Option<Object> {
        self.fetch_object_at_version(object_id, version)
            .ok()
            .flatten()
    }
}

pub fn prefetch_requests_for_checkpoint_contents(
    contents: &VersionedFullCheckpointContents,
) -> Result<BTreeSet<PrefetchRequest>> {
    let mut requests = BTreeSet::new();
    for execution_data in contents.iter() {
        requests.extend(prefetch_requests_for_transaction(
            execution_data.transaction.transaction_data(),
            &execution_data.effects,
        )?);
    }
    Ok(requests)
}

pub fn prefetch_requests_for_transaction(
    transaction_data: &TransactionData,
    effects: &TransactionEffects,
) -> Result<BTreeSet<PrefetchRequest>> {
    let mut requests = BTreeSet::new();
    for input in transaction_data
        .input_objects()
        .context("failed to compute transaction inputs")?
    {
        match input {
            InputObjectKind::MovePackage(package_id) => {
                requests.insert(PrefetchRequest::Package { package_id });
            }
            InputObjectKind::ImmOrOwnedMoveObject((object_id, version, _digest)) => {
                requests.insert(PrefetchRequest::ObjectVersion {
                    object_id,
                    version: version.value(),
                });
            }
            InputObjectKind::SharedMoveObject { id, .. } => {
                let version = shared_input_versions_from_effects(effects)
                    .get(&id)
                    .copied()
                    .ok_or_else(|| anyhow!("missing shared input version for object {id}"))?;
                requests.insert(PrefetchRequest::ObjectVersion {
                    object_id: id,
                    version,
                });
            }
        }
    }

    requests.extend(package_prefetches_from_transaction(transaction_data)?);
    Ok(requests)
}

fn build_checked_input_objects<S: ReplayStateStore>(
    store: &S,
    transaction_data: &TransactionData,
    effects: &TransactionEffects,
) -> Result<CheckedInputObjects> {
    let shared_versions = shared_input_versions_from_effects(effects);
    let mut input_results = Vec::new();
    for input in transaction_data
        .input_objects()
        .context("failed to compute transaction inputs")?
    {
        match input {
            InputObjectKind::MovePackage(package_id) => {
                let package = store
                    .get_package_object(&package_id)?
                    .ok_or_else(|| anyhow!("missing package object {package_id}"))?;
                input_results.push(ObjectReadResult::new(
                    InputObjectKind::MovePackage(package_id),
                    package.object().clone().into(),
                ));
            }
            InputObjectKind::ImmOrOwnedMoveObject((object_id, version, _digest)) => {
                let object = store
                    .fetch_object_at_version(&object_id, version)?
                    .ok_or_else(|| anyhow!("missing object {object_id}@{}", version.value()))?;
                input_results.push(ObjectReadResult::new(
                    InputObjectKind::ImmOrOwnedMoveObject(object.compute_object_reference()),
                    object.into(),
                ));
            }
            InputObjectKind::SharedMoveObject {
                id,
                initial_shared_version,
                mutability,
            } => {
                let version = shared_versions
                    .get(&id)
                    .copied()
                    .ok_or_else(|| anyhow!("missing shared input version for object {id}"))?;
                let object = store
                    .fetch_object_at_version(&id, SequenceNumber::from_u64(version))?
                    .ok_or_else(|| anyhow!("missing shared object {id}@{version}"))?;
                input_results.push(ObjectReadResult::new(
                    InputObjectKind::SharedMoveObject {
                        id,
                        initial_shared_version,
                        mutability,
                    },
                    object.into(),
                ));
            }
        }
    }

    Ok(CheckedInputObjects::new_for_replay(InputObjects::new(
        input_results,
    )))
}

fn shared_input_versions_from_effects(effects: &TransactionEffects) -> BTreeMap<ObjectID, u64> {
    let mut versions = BTreeMap::new();

    for input in effects.input_consensus_objects() {
        let (object_id, version) = match input {
            InputConsensusObject::Mutate((object_id, version, _digest))
            | InputConsensusObject::ReadOnly((object_id, version, _digest)) => (object_id, version),
            InputConsensusObject::ReadConsensusStreamEnded(object_id, version)
            | InputConsensusObject::MutateConsensusStreamEnded(object_id, version)
            | InputConsensusObject::Cancelled(object_id, version) => (object_id, version),
        };
        versions.insert(object_id, version.value());
    }

    for (object_id, kind) in effects.unchanged_consensus_objects() {
        let version = match kind {
            UnchangedConsensusKind::ReadOnlyRoot((version, _digest)) => Some(version.value()),
            UnchangedConsensusKind::MutateConsensusStreamEnded(version)
            | UnchangedConsensusKind::ReadConsensusStreamEnded(version)
            | UnchangedConsensusKind::Cancelled(version) => Some(version.value()),
            UnchangedConsensusKind::PerEpochConfig => None,
        };
        if let Some(version) = version {
            versions.entry(object_id).or_insert(version);
        }
    }

    versions
}

fn package_prefetches_from_transaction(
    transaction_data: &TransactionData,
) -> Result<BTreeSet<PrefetchRequest>> {
    let mut packages = BTreeSet::new();
    if let TransactionKind::ProgrammableTransaction(ptb) = transaction_data.kind() {
        for command in &ptb.commands {
            match command {
                Command::MoveCall(move_call) => {
                    packages.insert(PrefetchRequest::Package {
                        package_id: move_call.package,
                    });
                    for argument in &move_call.type_arguments {
                        let tag = argument
                            .to_type_tag()
                            .context("failed to resolve move-call type argument")?;
                        packages_from_type_tag(&tag, &mut packages);
                    }
                }
                Command::MakeMoveVec(type_input, _) => {
                    if let Some(type_input) = type_input {
                        let tag = type_input
                            .to_type_tag()
                            .context("failed to resolve vector type argument")?;
                        packages_from_type_tag(&tag, &mut packages);
                    }
                }
                Command::Publish(_, deps) => {
                    packages.extend(
                        deps.iter()
                            .copied()
                            .map(|package_id| PrefetchRequest::Package { package_id }),
                    );
                }
                Command::Upgrade(_, deps, package_id, _) => {
                    packages.insert(PrefetchRequest::Package {
                        package_id: *package_id,
                    });
                    packages.extend(
                        deps.iter()
                            .copied()
                            .map(|package_id| PrefetchRequest::Package { package_id }),
                    );
                }
                Command::TransferObjects(_, _)
                | Command::SplitCoins(_, _)
                | Command::MergeCoins(_, _) => {}
            }
        }
    }

    if let TransactionKind::ProgrammableTransaction(ptb) = transaction_data.kind() {
        for input in &ptb.inputs {
            if let CallArg::Object(ObjectArg::Receiving((object_id, version, _digest))) = input {
                packages.insert(PrefetchRequest::ObjectVersion {
                    object_id: *object_id,
                    version: version.value(),
                });
            }
        }
    }

    Ok(packages)
}

fn packages_from_type_tag(tag: &TypeTag, packages: &mut BTreeSet<PrefetchRequest>) {
    match tag {
        TypeTag::Struct(struct_tag) => {
            packages.insert(PrefetchRequest::Package {
                package_id: struct_tag.address.into(),
            });
            for type_param in &struct_tag.type_params {
                packages_from_type_tag(type_param, packages);
            }
        }
        TypeTag::Vector(inner) => packages_from_type_tag(inner, packages),
        TypeTag::Bool
        | TypeTag::U8
        | TypeTag::U16
        | TypeTag::U32
        | TypeTag::U64
        | TypeTag::U128
        | TypeTag::U256
        | TypeTag::Address
        | TypeTag::Signer => {}
    }
}

fn validate_child_owner(
    parent: &ObjectID,
    child: &ObjectID,
    object: Option<Object>,
) -> SuiResult<Option<Object>> {
    let Some(object) = object else {
        return Ok(None);
    };

    if object.owner != Owner::ObjectOwner((*parent).into()) {
        return Err(SuiErrorKind::InvalidChildObjectAccess {
            object: *child,
            given_parent: *parent,
            actual_owner: object.owner.clone(),
        }
        .into());
    }

    Ok(Some(object))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sui_types::object::Owner;

    fn test_object(object_id: ObjectID, version: u64, owner: Owner) -> Object {
        let object = Object::immutable_with_id_for_testing(object_id);
        let mut object = object;
        object.owner = owner;
        object
            .data
            .try_as_move_mut()
            .unwrap()
            .increment_version_to(version.into());
        object
    }

    #[test]
    fn versioned_memory_store_uses_latest_live_version() {
        let object_id = ObjectID::random();
        let older = test_object(object_id, 1, Owner::Immutable);
        let newer = test_object(object_id, 2, Owner::Immutable);

        let mut store = VersionedMemoryStore::new();
        store.insert_object(older.clone());
        store.insert_object(newer.clone());

        assert_eq!(store.get_object(&object_id).unwrap().version().value(), 2);
        assert_eq!(
            store
                .get_object_at_version(&object_id, SequenceNumber::from_u64(1))
                .unwrap()
                .version()
                .value(),
            1
        );
    }

    #[test]
    fn versioned_memory_store_does_not_regress_latest_live_version() {
        let object_id = ObjectID::random();
        let older = test_object(object_id, 1, Owner::Immutable);
        let newer = test_object(object_id, 2, Owner::Immutable);

        let mut store = VersionedMemoryStore::new();
        store.insert_object(newer);
        store.insert_object(older);

        assert_eq!(store.get_object(&object_id).unwrap().version().value(), 2);
    }

    #[test]
    fn read_child_object_respects_upper_bound() {
        let parent = ObjectID::random();
        let child = ObjectID::random();
        let owner = Owner::ObjectOwner(parent.into());
        let older = test_object(child, 3, owner.clone());
        let newer = test_object(child, 9, owner);

        let mut store = VersionedMemoryStore::new();
        store.insert_object(older);
        store.insert_object(newer);

        assert_eq!(
            store
                .read_child_object(&parent, &child, SequenceNumber::from_u64(5))
                .unwrap()
                .unwrap()
                .version()
                .value(),
            3
        );
    }
}
