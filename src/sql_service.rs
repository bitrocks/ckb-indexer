use crate::sql_indexer::{Result, SqlIndexer};
use ckb_jsonrpc_types::{BlockNumber, BlockView};
use futures::future::Future;
use jsonrpc_derive::rpc;
use log::info;
use sqlx::PgPool;
use std::thread;
use std::time::Duration;

pub struct SqlService {
    store: PgPool,
    poll_interval: Duration,
    listen_address: String,
}

impl SqlService {
    pub async fn new(
        database_url: &str,
        listen_address: &str,
        poll_interval: Duration,
    ) -> Result<Self> {
        let store = PgPool::builder().build(database_url).await?;
        Ok(Self {
            store,
            poll_interval,
            listen_address: listen_address.to_string(),
        })
    }

    pub async fn poll(&self, rpc_client: gen_client::Client) -> Result<()> {
        let indexer = SqlIndexer::new(self.store.clone(), 100, 1000);
        loop {
            if let Some((tip_number, tip_hash)) = indexer.tip().await? {
                if let Ok(Some(block)) = rpc_client
                    .get_block_by_number((tip_number + 1).into())
                    .wait()
                {
                    let block: ckb_types::core::BlockView = block.into();
                    if block.parent_hash() == tip_hash {
                        info!("append {}, {}", block.number(), block.hash());
                        indexer.append(&block).await?;
                    } else {
                        info!("rollback {}, {}", tip_number, tip_hash);
                        indexer.rollback().await?;
                    }
                } else {
                    thread::sleep(self.poll_interval);
                }
            } else {
                if let Ok(Some(block)) = rpc_client.get_block_by_number(0u64.into()).wait() {
                    indexer.append(&block.into()).await?;
                }
            }
        }
    }
}

#[rpc(client)]
pub trait CkbRpc {
    #[rpc(name = "get_block_by_number")]
    fn get_block_by_number(&self, _number: BlockNumber) -> RpcResult<Option<BlockView>>;
}
