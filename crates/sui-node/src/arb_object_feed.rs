// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result, anyhow};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use sui_config::ArbObjectFeedConfig;
use sui_core::arb_object_feed::{ArbObjectFeed, ArbTxObjectBatch};
#[cfg(unix)]
use tokio::io::AsyncWriteExt;
#[cfg(unix)]
use tokio::net::{UnixListener, UnixStream};
#[cfg(unix)]
use tokio::sync::{Mutex, mpsc};
#[cfg(unix)]
use tracing::{info, warn};

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
    sender: mpsc::Sender<ArbTxObjectBatch>,
}

#[cfg(unix)]
impl UdsArbObjectFeed {
    fn start(config: ArbObjectFeedConfig) -> Result<Arc<Self>> {
        prepare_socket_path(&config.socket_path)?;

        let listener = UnixListener::bind(&config.socket_path).with_context(|| {
            format!(
                "failed to bind arb object feed socket at {}",
                config.socket_path.display()
            )
        })?;

        let (sender, receiver) = mpsc::channel(config.channel_capacity);
        let current_stream = Arc::new(Mutex::new(None));
        let socket_path = config.socket_path.clone();

        tokio::spawn(Self::accept_loop(
            listener,
            Arc::clone(&current_stream),
            socket_path.clone(),
        ));
        tokio::spawn(Self::write_loop(
            receiver,
            current_stream,
            socket_path.clone(),
        ));

        info!(
            socket_path = %socket_path.display(),
            channel_capacity = config.channel_capacity,
            "started arb object feed socket"
        );

        Ok(Arc::new(Self { sender }))
    }

    async fn accept_loop(
        listener: UnixListener,
        current_stream: Arc<Mutex<Option<UnixStream>>>,
        socket_path: PathBuf,
    ) {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    *current_stream.lock().await = Some(stream);
                    info!(
                        socket_path = %socket_path.display(),
                        "arb object feed client connected"
                    );
                }
                Err(error) => {
                    warn!(
                        socket_path = %socket_path.display(),
                        ?error,
                        "arb object feed accept failed"
                    );
                }
            }
        }
    }

    async fn write_loop(
        mut receiver: mpsc::Receiver<ArbTxObjectBatch>,
        current_stream: Arc<Mutex<Option<UnixStream>>>,
        socket_path: PathBuf,
    ) {
        while let Some(batch) = receiver.recv().await {
            let payload = match bcs::to_bytes(&batch) {
                Ok(payload) => payload,
                Err(error) => {
                    warn!(tx_digest = ?batch.tx_digest, ?error, "failed to serialize arb object batch");
                    continue;
                }
            };

            let payload_len = match u32::try_from(payload.len()) {
                Ok(len) => len.to_be_bytes(),
                Err(_) => {
                    warn!(
                        tx_digest = ?batch.tx_digest,
                        payload_size = payload.len(),
                        "arb object batch is too large to frame"
                    );
                    continue;
                }
            };

            let mut guard = current_stream.lock().await;
            let Some(stream) = guard.as_mut() else {
                continue;
            };

            if let Err(error) = write_frame(stream, &payload_len, &payload).await {
                warn!(
                    socket_path = %socket_path.display(),
                    tx_digest = ?batch.tx_digest,
                    ?error,
                    "arb object feed client write failed"
                );
                *guard = None;
            }
        }
    }
}

#[cfg(unix)]
impl ArbObjectFeed for UdsArbObjectFeed {
    fn try_publish(&self, batch: ArbTxObjectBatch) {
        match self.sender.try_send(batch) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(batch)) => {
                warn!(
                    tx_digest = ?batch.tx_digest,
                    object_count = batch.objects.len(),
                    "arb object feed queue is full; dropping batch"
                );
            }
            Err(mpsc::error::TrySendError::Closed(batch)) => {
                warn!(
                    tx_digest = ?batch.tx_digest,
                    object_count = batch.objects.len(),
                    "arb object feed queue is closed; dropping batch"
                );
            }
        }
    }
}

#[cfg(unix)]
async fn write_frame(
    stream: &mut UnixStream,
    length: &[u8; 4],
    payload: &[u8],
) -> std::io::Result<()> {
    stream.write_all(length).await?;
    stream.write_all(payload).await?;
    stream.flush().await
}

#[cfg(unix)]
fn prepare_socket_path(path: &Path) -> Result<()> {
    use std::io::ErrorKind;
    use std::os::unix::fs::FileTypeExt;

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create arb object feed socket directory {}",
                parent.display()
            )
        })?;
    }

    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_socket() => {
            std::fs::remove_file(path).with_context(|| {
                format!(
                    "failed to remove stale arb object feed socket {}",
                    path.display()
                )
            })?;
        }
        Ok(_) => {
            return Err(anyhow!(
                "arb object feed path {} exists and is not a unix socket",
                path.display()
            ));
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "failed to inspect arb object feed socket path {}",
                    path.display()
                )
            });
        }
    }

    Ok(())
}
