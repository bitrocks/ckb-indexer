use super::Indexer;
use super::*;
use crate::store::{Batch, Error as StoreError, IteratorDirection, PostgresqlStore, Store};
use ckb_types::{
    core::{BlockNumber, BlockView},
    packed::{Byte32, Bytes, CellOutput, OutPoint, Script},
    prelude::*,
};
use postgres::{Client, NoTls};
use std::collections::HashMap;
use std::convert::TryInto;

pub struct SqlIndexer<S> {
    store: S,
    keep_num: u64,
    prune_interval: u64,
}

impl Indexer for SqlIndexer<PostgresqlStore> {
    fn new(db_config: &str, keep_num: u64, prune_interval: u64) -> Result<Self, Error> {
        let client = Client::connect(db_config, NoTls).unwrap();
        let store = PostgresqlStore::new(client)?;
        Ok(Self {
            store,
            keep_num,
            prune_interval,
        })
    }

    fn append(&self, block: &BlockView) -> Result<(), Error> {
        // 1. insert to block table
        // 2. insert to transaction table

        // 3. insert tx cell inputs --> find corresponding cell output, mark as comsumed

        // 4. insert tx output
        Ok(())
    }

    fn rollback(&self) -> Result<(), Error> {
        Ok(())
    }

    fn tip(&self) -> Result<Option<(BlockNumber, Byte32)>, Error> {
        // let (block_number, block_hash): (u64, Byte32) =
        //     sqlx::query("SELECT number, hash FROM blocks ORDER BY ID DESC LIMIT 1")
        //         .execute(&self.store.pool)
        //         .await?;
        Ok(None)
    }

    fn prune(&self) -> Result<(), Error> {
        Ok(())
    }
    fn get_live_cells_by_lock_script(&self, lock_script: &Script) -> Result<Vec<OutPoint>, Error> {
        Ok(vec![])
    }
    fn get_live_cells_by_type_script(&self, type_script: &Script) -> Result<Vec<OutPoint>, Error> {
        Ok(vec![])
    }
    fn get_transactions_by_lock_script(&self, lock_script: &Script) -> Result<Vec<Byte32>, Error> {
        Ok(vec![])
    }
    fn get_transactions_by_type_script(&self, type_script: &Script) -> Result<Vec<Byte32>, Error> {
        Ok(vec![])
    }
    fn get_detailed_live_cell(
        &self,
        out_point: &OutPoint,
    ) -> Result<Option<DetailedLiveCell>, Error> {
        Ok(None)
    }
    fn report(&self) -> Result<(), Error> {
        Ok(())
    }
}
