use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use jito_protos::bundle::{bundle_result::Result as BundleResultType, Bundle};
use jito_protos::convert::proto_packet_from_versioned_tx;
use jito_protos::searcher::searcher_service_client::SearcherServiceClient;
use jito_protos::searcher::{SendBundleRequest, SubscribeBundleResultsRequest};
use jito_searcher_client::get_searcher_client_auth;
use jito_searcher_client::token_authenticator::ClientInterceptor;
use solana_sdk::signature::{read_keypair_file, Keypair};
use solana_sdk::transaction::VersionedTransaction;
use tonic::codegen::InterceptedService;
use tonic::transport::Channel;
use tracing::{debug, info, warn};

use crate::config::Settings;

type Client = SearcherServiceClient<InterceptedService<Channel, ClientInterceptor>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Pending,
    Landed,
    Rejected,
    Dropped,
}

pub struct Relay {
    client: Client,
    outcomes: Arc<Mutex<HashMap<String, Outcome>>>,
}

impl Relay {
    pub async fn connect(settings: &Settings) -> Result<Self> {
        let keypair = read_keypair_file(&settings.jito_auth_keypair_path)
            .map_err(|e| anyhow::anyhow!("failed to read auth keypair: {}", e))?;
        let keypair: Arc<Keypair> = Arc::new(keypair);

        let client = get_searcher_client_auth(&settings.block_engine_url, &keypair)
            .await
            .context("failed to authenticate with block engine")?;
        info!(url = %settings.block_engine_url, "connected to block engine");

        Ok(Self {
            client,
            outcomes: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub async fn submit(&self, txs: Vec<VersionedTransaction>) -> Result<String> {
        let packets = txs.iter().map(proto_packet_from_versioned_tx).collect();

        let mut client = self.client.clone();
        let uuid = client
            .send_bundle(SendBundleRequest {
                bundle: Some(Bundle {
                    header: None,
                    packets,
                }),
            })
            .await
            .context("send_bundle failed")?
            .into_inner()
            .uuid;

        self.outcomes
            .lock()
            .unwrap()
            .insert(uuid.clone(), Outcome::Pending);
        debug!(%uuid, txs = txs.len(), "bundle submitted");
        Ok(uuid)
    }

    pub fn spawn_results_consumer(&self) -> tokio::task::JoinHandle<()> {
        let mut client = self.client.clone();
        let outcomes = Arc::clone(&self.outcomes);

        tokio::spawn(async move {
            let mut stream = match client
                .subscribe_bundle_results(SubscribeBundleResultsRequest {})
                .await
            {
                Ok(resp) => resp.into_inner(),
                Err(e) => {
                    warn!("subscribe_bundle_results failed: {}", e);
                    return;
                }
            };

            loop {
                match stream.message().await {
                    Ok(Some(msg)) => {
                        let outcome = match msg.result {
                            Some(BundleResultType::Processed(_))
                            | Some(BundleResultType::Finalized(_)) => Outcome::Landed,
                            Some(BundleResultType::Rejected(_)) => Outcome::Rejected,
                            Some(BundleResultType::Dropped(_)) => Outcome::Dropped,
                            Some(BundleResultType::Accepted(_)) | None => continue,
                        };
                        let mut map = outcomes.lock().unwrap();
                        if let Some(slot) = map.get_mut(&msg.bundle_id) {
                            *slot = outcome;
                            info!(bundle_id = %msg.bundle_id, ?outcome, "bundle result");
                        }
                    }
                    Ok(None) => {
                        warn!("bundle results stream closed");
                        break;
                    }
                    Err(e) => {
                        warn!("result feed: {}", e);
                        break;
                    }
                }
            }
        })
    }

    pub fn land_rate(&self) -> f64 {
        let map = self.outcomes.lock().unwrap();
        let landed = map.values().filter(|o| **o == Outcome::Landed).count();
        landed as f64 / map.len().max(1) as f64
    }
}
