use crate::{FlashblocksApi, cache::FlashblocksCache};
use alloy_primitives::{Address, TxHash, U256};
use flashblocks_compression::StreamDecoder;
use futures_util::StreamExt;
use jsonrpsee::core::async_trait;
use op_alloy_network::Optimism;
use reth_optimism_chainspec::OpChainSpec;
use reth_rpc_eth_api::{RpcBlock, RpcReceipt};
use rollup_boost_types::flashblocks::FlashblocksPayloadV1;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info};
use url::Url;

#[derive(Clone)]
pub struct FlashblocksOverlay {
    url: Url,
    cache: FlashblocksCache,
    decoder: Arc<StreamDecoder>,
}

impl FlashblocksOverlay {
    pub fn new(
        url: Url,
        chain_spec: Arc<OpChainSpec>,
        dict_oracle: Option<String>,
        dict_storage: Option<std::path::PathBuf>,
    ) -> Self {
        let decoder = StreamDecoder::new()
            .maybe_dict_storage(dict_storage.as_deref())
            .maybe_dict_oracle(dict_oracle.as_deref());

        Self {
            url,
            cache: FlashblocksCache::new(chain_spec),
            decoder: Arc::new(decoder),
        }
    }

    pub fn start(&mut self) -> eyre::Result<()> {
        let url = self.url.clone();
        let (sender, mut receiver) = mpsc::channel(100);
        let decoder = self.decoder.clone();

        tokio::spawn(async move {
            let mut backoff = std::time::Duration::from_secs(1);
            const MAX_BACKOFF: std::time::Duration = std::time::Duration::from_secs(10);

            loop {
                match connect_async(url.as_str()).await {
                    Ok((ws_stream, _)) => {
                        info!("WebSocket connection established");
                        let (_write, mut read) = ws_stream.split();

                        while let Some(msg) = read.next().await {
                            debug!("Received message: {:?}", msg);

                            match msg {
                                Ok(Message::Binary(bytes)) => {
                                    match try_decode_message(&bytes, &decoder) {
                                        Ok(payload) => {
                                            info!("Received payload: {:?}", payload);

                                            let _ = sender
                                                .send(InternalMessage::NewPayload(payload))
                                                .await
                                                .map_err(|e| {
                                                    error!(
                                                        "failed to send payload to channel: {}",
                                                        e
                                                    );
                                                });
                                        }
                                        Err(e) => {
                                            error!("failed to parse fb message: {}", e);
                                        }
                                    }
                                }
                                Ok(Message::Close(e)) => {
                                    error!("WebSocket connection closed: {:?}", e);
                                    break;
                                }
                                Err(e) => {
                                    error!("WebSocket connection error: {}", e);
                                    break;
                                }
                                _ => {}
                            }
                        }
                    }
                    Err(e) => {
                        error!(
                            "WebSocket connection error, retrying in {:?}: {}",
                            backoff, e
                        );
                        tokio::time::sleep(backoff).await;
                        // Double the backoff time, but cap at MAX_BACKOFF
                        backoff = std::cmp::min(backoff * 2, MAX_BACKOFF);
                        continue;
                    }
                }
            }
        });

        let cache_cloned = self.cache.clone();
        tokio::spawn(async move {
            while let Some(message) = receiver.recv().await {
                match message {
                    InternalMessage::NewPayload(payload) => {
                        if let Err(e) = cache_cloned.process_payload(payload) {
                            error!("failed to process payload: {}", e);
                        }
                    }
                }
            }
        });

        Ok(())
    }

    pub fn process_payload(&self, payload: FlashblocksPayloadV1) -> eyre::Result<()> {
        self.cache.process_payload(payload)
    }
}

enum InternalMessage {
    NewPayload(FlashblocksPayloadV1),
}

fn try_decode_message(bytes: &[u8], decoder: &StreamDecoder) -> eyre::Result<FlashblocksPayloadV1> {
    let raw = decoder.try_decode(bytes)?;
    serde_json::from_slice(&raw).map_err(|e| eyre::eyre!("failed to parse message: {}", e))
}

#[async_trait]
impl FlashblocksApi for FlashblocksOverlay {
    async fn block_by_number(&self, full: bool) -> Option<RpcBlock<Optimism>> {
        self.cache.get_block(full)
    }

    async fn get_transaction_receipt(&self, tx_hash: TxHash) -> Option<RpcReceipt<Optimism>> {
        self.cache.get_receipt(&tx_hash)
    }

    async fn get_balance(&self, address: Address) -> Option<U256> {
        self.cache.get_balance(address)
    }

    async fn get_transaction_count(&self, address: Address) -> Option<u64> {
        self.cache.get_transaction_count(address)
    }
}
