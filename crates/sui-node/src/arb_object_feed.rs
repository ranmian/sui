// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use sui_config::ArbObjectFeedConfig;
use sui_core::arb_object_feed::{ArbObjectDatagram, ArbObjectFeed, ArbTxObjectBatch};
#[cfg(unix)]
use tokio::net::UnixDatagram;
#[cfg(unix)]
use tracing::{debug, info, warn};

#[cfg(not(unix))]
pub(crate) fn build_arb_object_feed(
    config: Option<&ArbObjectFeedConfig>,
) -> Result<Option<Arc<dyn ArbObjectFeed>>> {
    if config.is_some() {
        return Err(anyhow!(
            "arb object feed currently requires unix domain socket support"
        ));
    }

    Ok(None)
}

#[cfg(unix)]
pub(crate) fn build_arb_object_feed(
    config: Option<&ArbObjectFeedConfig>,
) -> Result<Option<Arc<dyn ArbObjectFeed>>> {
    let Some(config) = config.cloned() else {
        return Ok(None);
    };

    Ok(Some(UdsArbObjectFeed::start(config)?))
}

struct UdsArbObjectFeed {
    socket: UnixDatagram,
    socket_path: PathBuf,
}

#[cfg(unix)]
impl UdsArbObjectFeed {
    fn start(config: ArbObjectFeedConfig) -> Result<Arc<Self>> {
        ensure_socket_parent_dir(&config.socket_path)?;
        let socket = UnixDatagram::unbound().context("failed to create arb object feed socket")?;
        let socket_path = config.socket_path.clone();

        info!(
            destination_socket_path = %socket_path.display(),
            channel_capacity = config.channel_capacity,
            "started arb object feed datagram sender"
        );

        Ok(Arc::new(Self {
            socket,
            socket_path,
        }))
    }
}

#[cfg(unix)]
impl ArbObjectFeed for UdsArbObjectFeed {
    fn try_publish(&self, batch: ArbTxObjectBatch) {
        for datagram in batch.into_datagrams() {
            send_datagram(&self.socket, &self.socket_path, datagram);
        }
    }
}

#[cfg(unix)]
fn ensure_socket_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create arb object feed socket directory {}",
                parent.display()
            )
        })?;
    }

    Ok(())
}

#[cfg(unix)]
fn send_datagram(socket: &UnixDatagram, socket_path: &Path, datagram: ArbObjectDatagram) {
    let payload = match bcs::to_bytes(&datagram) {
        Ok(payload) => payload,
        Err(error) => {
            warn!(
                tx_digest = ?datagram.tx_digest,
                object_id = %datagram.object.object_id,
                ?error,
                "failed to serialize arb object datagram"
            );
            return;
        }
    };

    match socket.try_send_to(&payload, socket_path) {
        Ok(sent) if sent == payload.len() => {}
        Ok(sent) => {
            warn!(
                destination_socket_path = %socket_path.display(),
                tx_digest = ?datagram.tx_digest,
                object_id = %datagram.object.object_id,
                sent_bytes = sent,
                payload_size = payload.len(),
                "arb object datagram was only partially sent; dropping remainder"
            );
        }
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::WouldBlock
                    | std::io::ErrorKind::NotFound
                    | std::io::ErrorKind::ConnectionRefused
            ) =>
        {
            debug!(
                destination_socket_path = %socket_path.display(),
                tx_digest = ?datagram.tx_digest,
                object_id = %datagram.object.object_id,
                ?error,
                "arb object datagram dropped"
            );
        }
        Err(error) => {
            warn!(
                destination_socket_path = %socket_path.display(),
                tx_digest = ?datagram.tx_digest,
                object_id = %datagram.object.object_id,
                ?error,
                "arb object datagram send failed"
            );
        }
    }
}
