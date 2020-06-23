use crate::store::{Batch, Error as StoreError, IteratorDirection, Store};
use async_std::task;
use bigdecimal::BigDecimal;
use bigdecimal::ToPrimitive;
use ckb_types::{
    core::{BlockNumber, BlockView},
    packed::{self, Byte32, Bytes, CellOutput, OutPoint, Script, Uint32},
    prelude::*,
};
use sqlx::PgPool;
use sqlx_core::postgres::PgQueryAs;
use std::convert::TryInto;
pub type Result<T> = std::result::Result<T, sqlx::Error>;

pub struct SqlIndexer {
    store: PgPool,
    // number of blocks to keep for rollback and forking, for example:
    // keep_num: 100, current tip: 321, will prune ConsumedOutPoint / TxHash kv pair whiches block_number <= 221
    keep_num: u64,
    prune_interval: u64,
}

#[derive(sqlx::FromRow)]
pub struct BlockInfo {
    block_number: BigDecimal,
    block_hash: Vec<u8>,
}

impl SqlIndexer {
    pub fn new(store: PgPool, keep_num: u64, prune_interval: u64) -> Self {
        Self {
            store,
            keep_num,
            prune_interval,
        }
    }

    pub fn store(&self) -> &PgPool {
        &self.store
    }

    /// Append a new block to SQL database.
    ///
    /// # Errors
    ///
    /// It propagates sqlx::Error
    pub async fn append(&self, block: &BlockView) -> Result<()> {
        println!("Start append!");
        let mut db_tx = self.store.begin().await?;
        let block_hash = block.hash();

        let block_number = block.number();
        let parent_hash = block.parent_hash();

        let row = sqlx::query!(
            "INSERT INTO block_digests (block_hash, block_number, parent_hash) VALUES($1, $2, $3) RETURNING id",
        block_hash.as_slice(),
        BigDecimal::from(block_number),
        parent_hash.as_slice())
        .fetch_one(&mut db_tx)
        .await?;
        println!("INSERT block_digests: {:?}", row);

        let block_id = row.id;

        let transactions = block.transactions();

        for (tx_index, tx) in transactions.iter().enumerate() {
            let tx_index = tx_index as u32;
            let tx_hash = tx.hash();

            // insert transaction_digest
            let row = sqlx::query!(
                "INSERT INTO transaction_digests (tx_hash, block_id) VALUES ($1, $2) RETURNING id",
                tx_hash.as_slice(),
                block_id
            )
            .fetch_one(&mut db_tx)
            .await?;

            println!("INSERT transaction_digests result: {:?}", row);
            let tx_id = row.id;

            if tx_index > 0 {
                for (input_index, input) in tx.inputs().into_iter().enumerate() {
                    let input_index = input_index as u32;
                    let out_point = input.previous_output();
                    let out_point_tx_hash = out_point.tx_hash();
                    // mark corresponding cell as comsumed
                    sqlx::query!("UPDATE cells SET consumed = true WHERE tx_hash = $1 AND index = $2 AND consumed = false"
                    ,out_point_tx_hash.as_slice()
                    ,u32::from_le_bytes(out_point.index().as_slice().try_into().expect("slice with incorrect length")) as i32)
                    // TODO: make unpack works
                    // , Unpack::<packed::Uint32>::unpack(&out_point.index().as_reader())
                    .execute(&mut db_tx)
                    .await?;
                    println!("UPDATE cells");

                    // insert to `transaction_inputs` table
                    sqlx::query!("INSERT INTO transaction_inputs (tx_id, tx_hash, index) VALUES ($1, $2, $3)"
                    ,tx_id
                    ,tx_hash.as_slice()
                    ,input_index as i32)
                    .execute(&mut db_tx)
                    .await?;
                    println!("INSERT transaction_inputs");
                }
            }

            for (output_index, output) in tx.outputs().into_iter().enumerate() {
                // insert to scripts as lock script
                let lock_script = output.lock();
                let lock_script_hash = lock_script.calc_script_hash();
                let code_hash = lock_script.code_hash();
                let hash_type = lock_script.hash_type();
                let args = lock_script.args();
                let row = sqlx::query!(
                    "WITH temp AS(
                        INSERT INTO scripts (script_hash, code_hash, hash_type, args)
                        VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING
                        RETURNING id
                    )
                    SELECT * FROM temp
                    UNION
                    SELECT id FROM scripts WHERE script_hash = $1",
                    lock_script_hash.as_slice(),
                    code_hash.as_slice(),
                    hash_type.as_slice()[0] as i32,
                    args.as_slice()
                )
                .fetch_one(&mut db_tx)
                .await?;
                let lock_script_id = row.id;

                // insert to scripts as type script
                let type_script_id = if let Some(type_script) = output.type_().to_opt() {
                    let type_script_hash = type_script.calc_script_hash();
                    let code_hash = type_script.code_hash();
                    let hash_type = type_script.hash_type();
                    let args = type_script.args();
                    let row = sqlx::query!(
                        "WITH temp AS(
                            INSERT INTO scripts (script_hash, code_hash, hash_type, args)
                            VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING
                            RETURNING id
                        )
                        SELECT * FROM temp
                        UNION
                        SELECT id FROM scripts WHERE script_hash = $1",
                        type_script_hash.as_slice(),
                        code_hash.as_slice(),
                        hash_type.as_slice()[0] as i32,
                        args.as_slice()
                    )
                    .fetch_one(&mut db_tx)
                    .await?;
                    println!("row info: {:?}", row);
                    let type_script_id = row.id;
                    sqlx::query!("INSERT INTO cells (capacity, lock_script_id, type_script_id, tx_id, tx_hash, index, block_number) 
                    VALUES ($1,$2,$3,$4,$5,$6, $7)"
                    ,BigDecimal::from(u64::from_le_bytes(output.capacity().as_slice().try_into().expect("slice with incorrect length")))
                    ,lock_script_id
                    ,type_script_id
                    ,tx_id
                    ,tx_hash.as_slice()
                    ,output_index as i32
                    ,BigDecimal::from(block_number))
                    .execute(&mut db_tx).await?
                } else {
                    // insert to cell
                    sqlx::query!("INSERT INTO cells (capacity, lock_script_id, tx_id, tx_hash, index, block_number) 
                VALUES ($1,$2,$3,$4,$5,$6) "
                ,BigDecimal::from(u64::from_le_bytes(output.capacity().as_slice().try_into().expect("slice with incorrect length")))
                ,lock_script_id
                ,tx_id
                ,tx_hash.as_slice()
                ,output_index as i32
                ,BigDecimal::from(block_number))
                .execute(&mut db_tx).await?
                };
            }
        }

        db_tx.commit().await?;
        Ok(())
    }
    pub async fn tip(&self) -> Result<Option<(BlockNumber, Byte32)>> {
        let block_info = sqlx::query_as::<_, BlockInfo>(
            "SELECT block_number,block_hash FROM block_digests ORDER BY block_number DESC LIMIT 1",
        )
        .fetch_optional(&self.store)
        .await?;
        match block_info {
            Some(BlockInfo {
                block_number,
                block_hash,
            }) => {
                let block_number_u64 = block_number.to_u64().unwrap();
                let block_hash_byte32 = Byte32::from_slice(&block_hash).unwrap();
                Ok(Some((block_number_u64, block_hash_byte32)))
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ckb_jsonrpc_types::{
        BlockNumber, BlockView, CellOutput, JsonBytes, OutPoint, Script, Uint32,
    };
    use futures::Future;
    use hyper::rt;
    use jsonrpc_core_client::transports::http;
    use jsonrpc_derive::rpc;

    #[rpc(client)]
    pub trait CkbRpc {
        #[rpc(name = "get_block_by_number")]
        fn get_block_by_number(&self, _number: BlockNumber) -> Result<Option<BlockView>>;
    }

    #[derive(sqlx::FromRow)]
    pub struct BlockInfo {
        block_number: BigDecimal,
        block_hash: Vec<u8>,
    }
    async fn get_tip() -> Result<Option<BlockInfo>> {
        let database_url = "postgres://hupeng:default@localhost/ckb_indexer";
        let pool = PgPool::builder().build(database_url).await?;
        let indexer = SqlIndexer::new(pool, 100, 10000);
        let block_info = sqlx::query_as::<_, BlockInfo>(
            "SELECT block_number,block_hash FROM block_digests ORDER BY block_number DESC LIMIT 1",
        )
        .fetch_optional(&indexer.store)
        .await?;
        match block_info {
            Some(block_info) => {
                let block_number = &block_info.block_number;
                let block_hash = &block_info.block_hash;
                println!("result: {:?}, {:?}", block_number, block_hash);
                Ok(Some(block_info))
            }
            None => Ok(None),
        }
    }

    #[test]
    fn query_table_works() {
        task::block_on(async {
            let future = get_tip().await;
            match future {
                Ok(_) => println!("yes"),
                Err(e) => println!("Error: {:?}", e),
            }
        });
        assert!(false)
    }

    #[test]
    fn append_block_works() {
        // TODO
        // Change to local test
        rt::run(rt::lazy(move || {
            let uri = "http://127.0.0.1:8114";
            http::connect(uri)
                .and_then(move |client: gen_client::Client| {
                    task::block_on(async {
                        let database_url = "postgres://hupeng:default@localhost/ckb_indexer_dev";
                        let pool = PgPool::builder().build(database_url).await.unwrap();
                        let indexer = SqlIndexer::new(pool, 100, 10000);
                        println!("Before------------------");
                        if let Ok(Some(block)) = client.get_block_by_number(0.into()).wait() {
                            println!("block: {:?}", block);
                            let result = indexer.append(&block.into()).await;
                            match result {
                                Ok(_) => println!("Done!"),
                                Err(e) => println!("Error: {:?}", e),
                            }
                        }
                    });
                    Ok(())
                })
                .map_err(|e| {
                    println!("Error: {:?}", e);
                })
        }));
        assert!(false)
    }

    #[test]
    fn append_and_rollback_to_empty() {}
}
