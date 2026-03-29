// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use move_core_types::language_storage::StructTag;
use serde::{Deserialize, Serialize};
use sui_types::base_types::{ObjectID, SequenceNumber};
use sui_types::digests::{ObjectDigest, TransactionDigest};
use sui_types::inner_temporary_store::WrittenObjects;
use sui_types::messages_checkpoint::CheckpointSequenceNumber;
use sui_types::object::{Object, Owner};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ArbObjectUpdate {
    pub object_id: ObjectID,
    pub version: SequenceNumber,
    pub digest: ObjectDigest,
    pub owner: Owner,
    pub struct_tag: StructTag,
    pub contents_bcs: Vec<u8>,
}

impl ArbObjectUpdate {
    pub fn from_object(object: &Object) -> Option<Self> {
        let move_object = object.data.try_as_move()?;
        let struct_tag = object.struct_tag()?;

        Some(Self {
            object_id: object.id(),
            version: object.version(),
            digest: object.digest(),
            owner: object.owner().clone(),
            struct_tag,
            contents_bcs: move_object.contents().to_vec(),
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ArbTxObjectBatch {
    pub checkpoint_seq: Option<CheckpointSequenceNumber>,
    pub tx_digest: TransactionDigest,
    pub objects: Vec<ArbObjectUpdate>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ArbObjectDatagram {
    pub checkpoint_seq: Option<CheckpointSequenceNumber>,
    pub tx_digest: TransactionDigest,
    pub object: ArbObjectUpdate,
}

impl ArbTxObjectBatch {
    pub fn from_written_objects(
        checkpoint_seq: Option<CheckpointSequenceNumber>,
        tx_digest: TransactionDigest,
        written: &WrittenObjects,
    ) -> Option<Self> {
        let objects: Vec<_> = written
            .values()
            .filter_map(ArbObjectUpdate::from_object)
            .collect();

        (!objects.is_empty()).then_some(Self {
            checkpoint_seq,
            tx_digest,
            objects,
        })
    }

    pub fn into_datagrams(self) -> impl Iterator<Item = ArbObjectDatagram> {
        let checkpoint_seq = self.checkpoint_seq;
        let tx_digest = self.tx_digest;

        self.objects
            .into_iter()
            .map(move |object| ArbObjectDatagram {
                checkpoint_seq,
                tx_digest: tx_digest.clone(),
                object,
            })
    }
}

pub trait ArbObjectFeed: Send + Sync {
    fn try_publish(&self, batch: ArbTxObjectBatch);
}
