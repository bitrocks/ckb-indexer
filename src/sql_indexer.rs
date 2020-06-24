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
const SCRIPT_TYPE_LOCK: i32 = 0;
const SCRIPT_TYPE_TYPE: i32 = 1;
const IO_TYPE_INPUT: i32 = 0;
const IO_TYPE_OUTPUT: i32 = 1;
pub struct SqlIndexer {
    store: PgPool,
    // number of blocks to keep for rollback and forking, for example:
    // keep_num: 100, current tip: 321, will prune ConsumedOutPoint / TxHash kv pair whiches block_number <= 221
    keep_num: u64,
    prune_interval: u64,
}

#[derive(sqlx::FromRow)]
pub struct BlockDigest {
    block_number: BigDecimal,
    block_hash: Vec<u8>,
}

pub struct DetailedLiveCell {
    pub block_number: BlockNumber,
    pub block_hash: Byte32,
    pub tx_index: u32,
    pub cell_output: CellOutput,
    pub cell_data: Bytes,
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
    /// # Steps
    ///
    /// 1. insert into `block_digests` table;
    /// 2. for every txns:
    ///     1. insert into `transaction_digests` table;
    ///     2. for every inputs:
    ///         1. insert into `transaction_inputs` table except cellbase tx;
    ///         2. find corresponding `cell` and mark the `consumed` field as true;
    ///     3. for every outputs:
    ///         1. insert into `scripts` table;
    ///         2. insert into `transaction_scripts' table;
    ///         3. insert into `cells` table;
    ///
    /// Bundles those steps in one database transaction.
    ///
    /// # Errors
    ///
    /// It propagates sqlx::Error
    pub async fn append(&self, block: &BlockView) -> Result<()> {
        let mut db_tx = self.store.begin().await?;
        let block_hash: Byte32 = block.hash();

        let block_number: BigDecimal = BigDecimal::from(block.number());

        sqlx::query!(
            "INSERT INTO block_digests (block_hash, block_number) VALUES($1, $2)",
            block_hash.as_slice(),
            block_number
        )
        .execute(&mut db_tx)
        .await?;

        let transactions = block.transactions();

        for (tx_index, tx) in transactions.iter().enumerate() {
            let tx_index: i32 = tx_index as i32;
            let tx_hash: Byte32 = tx.hash();

            // insert transaction_digest
            let row = sqlx::query!(
                "INSERT INTO transaction_digests (tx_hash, tx_index, output_count, block_number) VALUES ($1, $2, $3, $4) returning id",
                tx_hash.as_slice(),
                tx_index,
                tx.outputs().len() as i32,
                block_number
            )
            .fetch_one(&mut db_tx)
            .await?;

            let tx_id = row.id;

            if tx_index > 0 {
                for (input_index, input) in tx.inputs().into_iter().enumerate() {
                    let input_index: i32 = input_index as i32;
                    let out_point = input.previous_output();
                    let out_point_tx_hash = out_point.tx_hash();
                    // mark corresponding cell as comsumed
                    let cell = sqlx::query!("UPDATE cells SET consumed = true WHERE tx_hash = $1 AND index = $2 AND consumed = false RETURNING *"
                    ,out_point_tx_hash.as_slice()
                    ,u32::from_le_bytes(out_point.index().as_slice().try_into().expect("slice with incorrect length")) as i32)
                    // FIXME
                    // , Unpack::<packed::Uint32>::unpack(&out_point.index().as_reader())
                    .fetch_one(&mut db_tx)
                    .await?;

                    // insert previous output into `transaction_scripts` table
                    sqlx::query!("INSERT INTO transaction_scripts (script_type, io_type, index, transaction_digest_id, script_id) 
                    VALUES($1,$2,$3,$4,$5)", 
                    SCRIPT_TYPE_LOCK, IO_TYPE_INPUT, input_index, tx_id, cell.lock_script_id)
                .execute(&mut db_tx).await?;

                    if let Some(type_script_id) = cell.type_script_id {
                        sqlx::query!("INSERT INTO transaction_scripts (script_type, io_type, index, transaction_digest_id, script_id) 
                    VALUES($1,$2,$3,$4,$5)", 
                    SCRIPT_TYPE_TYPE, IO_TYPE_INPUT, input_index, tx_id, type_script_id)
                .execute(&mut db_tx).await?;
                    }
                    // insert to `transaction_inputs` table
                    sqlx::query!("INSERT INTO transaction_inputs (transaction_digest_id, previous_tx_hash, previous_index) VALUES ($1, $2, $3)"
                    ,tx_id
                    ,tx_hash.as_slice()
                    ,input_index as i32)
                    .execute(&mut db_tx)
                    .await?;
                }
            }

            for (output_index, output) in tx.outputs().into_iter().enumerate() {
                // insert to scripts as lock script
                let output_index: i32 = output_index as i32;
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

                // insert to transaction_scripts
                sqlx::query!("INSERT INTO transaction_scripts (script_type, io_type, index, transaction_digest_id, script_id) 
                    VALUES($1,$2,$3,$4,$5)", 
                    SCRIPT_TYPE_LOCK, IO_TYPE_OUTPUT, output_index, tx_id, lock_script_id)
                .execute(&mut db_tx).await?;

                // insert to scripts as type script
                if let Some(type_script) = output.type_().to_opt() {
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
                    let type_script_id = row.id;
                    sqlx::query!("INSERT INTO cells (capacity, lock_script_id, type_script_id, transaction_digest_id, tx_hash, index, block_number, tx_index) 
                    VALUES ($1,$2,$3,$4,$5,$6, $7, $8)",
                    BigDecimal::from(u64::from_le_bytes(output.capacity().as_slice().try_into().expect("slice with incorrect length"))),
                    lock_script_id,
                    type_script_id,
                    tx_id,
                    tx_hash.as_slice(),
                    output_index,
                    block_number ,
                    tx_index)
                    .execute(&mut db_tx).await?;
                    // insert to transaction_scripts
                    sqlx::query!("INSERT INTO transaction_scripts (script_type, io_type, index, transaction_digest_id, script_id) 
                    VALUES($1,$2,$3,$4,$5)", 
                    SCRIPT_TYPE_TYPE, IO_TYPE_OUTPUT, output_index, tx_id, type_script_id)
                .execute(&mut db_tx).await?
                } else {
                    // insert to cell
                    sqlx::query!("INSERT INTO cells (capacity, lock_script_id, transaction_digest_id, tx_hash, index, block_number, tx_index) 
                VALUES ($1,$2,$3,$4,$5,$6, $7) "
                ,BigDecimal::from(u64::from_le_bytes(output.capacity().as_slice().try_into().expect("slice with incorrect length")))
                ,lock_script_id
                ,tx_id
                ,tx_hash.as_slice()
                ,output_index as i32
                ,block_number
                ,tx_index)
                .execute(&mut db_tx).await?
                };
            }
        }

        db_tx.commit().await?;

        if block.number() % self.prune_interval == 0 {
            self.prune().await?;
        }
        Ok(())
    }

    /// Rollback current tip block in database.
    ///
    /// # Steps
    ///
    /// ## Revert append procedure
    /// 1. for every txns(reverse):
    ///     1. for every inputs:
    ///         1. find corresponding `cell` and revert the `consumed` field to false;
    ///         2. delete input from `transaction_inputs` table;
    ///     2. for every outputs:
    ///         1. delete cell from `cells` table;
    ///         2. delete transaction_script from `transaction_scripts` table;
    ///         3. delete lock/type script from `scrpts` table;
    ///     3. delete transaction_digest from `transaction_digests` table;
    /// 2. delete block_digest from `block_digests` table;
    ///
    /// ## Another rollback procedure
    ///
    /// 1. for every txns(reverse):
    ///     1. for every inputs:
    ///         1. find corresponding `cell` and revert the `consumed` field to false;
    /// 2. delete block_digest from `block_digests` table CASCADE;
    ///
    /// # Errors
    ///
    /// It propagates sqlx::Error
    pub async fn rollback(&self) -> Result<()> {
        if let Some((block_number, _block_hash)) = self.tip().await? {
            let mut db_tx = self.store.begin().await?;
            let block_number: BigDecimal = BigDecimal::from(block_number);
            let txs = sqlx::query!(
                "SELECT * FROM transaction_digests WHERE block_number = $1 ORDER BY tx_index DESC",
                block_number
            )
            .fetch_all(&mut db_tx)
            .await?;
            for tx in txs.into_iter() {
                let tx_inputs = sqlx::query!(
                    "SELECT * FROM transaction_inputs WHERE transaction_digest_id = $1",
                    tx.id
                )
                .fetch_all(&mut db_tx)
                .await?;
                for tx_input in tx_inputs.into_iter() {
                    sqlx::query!(
                        "UPDATE cells SET consumed = false WHERE tx_hash = $1 AND index = $2",
                        tx_input.previous_tx_hash,
                        tx_input.previous_index
                    )
                    .execute(&mut db_tx)
                    .await?;
                }
            }
            // It will delete rows in transaction_digests/transaction_inputs/cells/transaction_scripts CASCADE.
            sqlx::query!(
                "DELETE FROM block_digests WHERE block_number = $1",
                block_number
            )
            .execute(&mut db_tx)
            .await?;
            db_tx.commit().await?;
        }
        Ok(())
    }

    /// Prune consomed cells and transaction_inputs before `prune_to_block` height.
    ///
    /// # Errors
    ///
    /// It propagates sqlx::Error
    pub async fn prune(&self) -> Result<()> {
        if let Some((tip_number, _block_hash)) = self.tip().await? {
            if tip_number > self.keep_num {
                let prune_to_block = BigDecimal::from(tip_number - self.keep_num);
                let mut db_tx = self.store.begin().await?;
                sqlx::query!(
                    "DELETE FROM cells WHERE block_number < $1 and consumed = true",
                    prune_to_block
                )
                .execute(&mut db_tx)
                .await?;
                sqlx::query!("DELETE FROM transaction_inputs WHERE transaction_digest_id IN (SELECT id FROM transaction_digests WHERE block_number < $1)", prune_to_block).execute(&mut db_tx).await?;
                db_tx.commit().await?;
            }
        }
        Ok(())
    }

    /// Fetch the latest blocks' number and hash if exists.
    ///
    /// # Errors
    ///
    /// It propagates sqlx::Error
    pub async fn tip(&self) -> Result<Option<(BlockNumber, Byte32)>> {
        let block_info = sqlx::query_as::<_, BlockDigest>(
            "SELECT block_number,block_hash FROM block_digests ORDER BY block_number DESC LIMIT 1",
        )
        .fetch_optional(&self.store)
        .await?;
        match block_info {
            Some(BlockDigest {
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

    pub async fn get_live_cells_by_lock_script(
        &self,
        lock_script: &Script,
    ) -> Result<Vec<OutPoint>> {
        let lock_script_hash = lock_script.calc_script_hash();
        let cells = sqlx::query!(
            "
       SELECT tx_hash, index FROM cells 
       JOIN scripts 
       ON cells.lock_script_id = scripts.id AND scripts.script_hash = $1 
       ORDER BY cells.block_number ASC",
            lock_script_hash.as_slice()
        )
        .fetch_all(&self.store)
        .await?;
        let mut out_points: Vec<OutPoint> = vec![];
        for cell in cells.into_iter() {
            let index = cell.index;
            let tx_hash: Byte32 = Byte32::from_slice(&cell.tx_hash).unwrap();
            // FIXME: i32 to Uint32
            //    let index: Uint32 =  Uint32::from_slice(index::to_le_bytes()).unwrap();
            let out_point = OutPoint::new_builder().tx_hash(tx_hash).build();
            out_points.push(out_point)
        }
        Ok(out_points)
    }
    pub async fn get_live_cells_by_type_script(
        &self,
        type_script: &Script,
    ) -> Result<Vec<OutPoint>> {
        let type_script_hash = type_script.calc_script_hash();
        let cells = sqlx::query!(
            "
        SELECT tx_hash, index FROM cells 
        JOIN scripts 
        ON cells.type_script_id = scripts.id AND scripts.script_hash = $1 
        ORDER BY cells.block_number ASC",
            type_script_hash.as_slice()
        )
        .fetch_all(&self.store)
        .await?;
        let mut out_points: Vec<OutPoint> = vec![];
        for cell in cells.into_iter() {
            let index = cell.index;
            let tx_hash: Byte32 = Byte32::from_slice(&cell.tx_hash).unwrap();
            // FIXME: i32 to Uint32
            //    let index: Uint32 =  Uint32::from_slice(index::to_le_bytes()).unwrap();
            let out_point = OutPoint::new_builder().tx_hash(tx_hash).build();
            out_points.push(out_point)
        }
        Ok(out_points)
    }

    pub async fn get_transactions_by_lock_script(
        &self,
        lock_script: &Script,
    ) -> Result<Vec<Byte32>> {
        let lock_script_hash = lock_script.calc_script_hash();
        let tx_hashs = sqlx::query!(
            "
        SELECT tx_hash FROM transaction_digests 
        JOIN transaction_scripts 
        ON transaction_digests.id = transaction_scripts.transaction_digest_id 
        JOIN scripts 
        ON transaction_scripts.script_id = scripts.id AND scripts.script_hash = $1
        ORDER BY transaction_digests.block_number ASC",
            lock_script_hash.as_slice()
        )
        .fetch_all(&self.store)
        .await?;
        let mut tx_hashs_byte32: Vec<Byte32> = vec![];
        for tx_hash in tx_hashs.into_iter() {
            let tx_hash: Byte32 = Byte32::from_slice(&tx_hash.tx_hash).unwrap();
            tx_hashs_byte32.push(tx_hash)
        }
        Ok(tx_hashs_byte32)
    }

    pub async fn get_transactions_by_type_script(
        &self,
        type_script: &Script,
    ) -> Result<Vec<Byte32>> {
        let type_script_hash = type_script.calc_script_hash();
        let tx_hashs = sqlx::query!(
            "
        SELECT tx_hash FROM transaction_digests 
        JOIN transaction_scripts 
        ON transaction_digests.id = transaction_scripts.transaction_digest_id 
        JOIN scripts 
        ON transaction_scripts.script_id = scripts.id AND scripts.script_hash = $1
        ORDER BY transaction_digests.block_number ASC",
            type_script_hash.as_slice()
        )
        .fetch_all(&self.store)
        .await?;
        let mut tx_hashs_byte32: Vec<Byte32> = vec![];
        for tx_hash in tx_hashs.into_iter() {
            let tx_hash: Byte32 = Byte32::from_slice(&tx_hash.tx_hash).unwrap();
            tx_hashs_byte32.push(tx_hash)
        }
        Ok(tx_hashs_byte32)
    }

    pub async fn get_detailed_live_cell(
        &self,
        out_point: &OutPoint,
    ) -> Result<Option<DetailedLiveCell>> {
        let tx_hash = out_point.tx_hash();
        let index = out_point.index();
        let cell = sqlx::query!(
            "SELECT * FROM cells WHERE tx_hash = $1 AND index = $2",
            tx_hash.as_slice(),
            u32::from_le_bytes(
                index
                    .as_slice()
                    .try_into()
                    .expect("slice with incorrect length")
            ) as i32
        )
        .fetch_optional(&self.store)
        .await?;
        // TODO construct detailed_live_cell
        Ok(None)
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
    fn rollback_block_works() {
        // TODO
        // Change to local test
        task::block_on(async {
            let database_url = "postgres://hupeng:default@localhost/ckb_indexer_dev";
            let pool = PgPool::builder().build(database_url).await.unwrap();
            let indexer = SqlIndexer::new(pool, 100, 10000);
            println!("Before------------------");
            let result = indexer.rollback().await;
            match result {
                Ok(_) => println!("Done!"),
                Err(e) => println!("Error: {:?}", e),
            }
        });
        assert!(false)
    }

    #[test]
    fn append_and_rollback_to_empty() {}
}
