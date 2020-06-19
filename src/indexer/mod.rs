use crate::store::{Error as StoreError, Store};
use ckb_types::{
    core::{BlockNumber, BlockView},
    packed::{Byte32, Bytes, CellOutput, OutPoint, Script},
    prelude::*,
};

pub mod kv_indexer;
pub mod sql_indexer;

pub type TxIndex = u32;
pub type OutputIndex = u32;
pub type IOIndex = u32;
pub enum IOType {
    Input,
    Output,
}

pub struct DetailedLiveCell {
    pub block_number: BlockNumber,
    pub block_hash: Byte32,
    pub tx_index: TxIndex,
    pub cell_output: CellOutput,
    pub cell_data: Bytes,
}

pub trait Indexer {
    fn new(db_config: &str, keep_num: u64, prune_interval: u64) -> Result<Self, Error>
    where
        Self: Sized;
    fn append(&self, block: &BlockView) -> Result<(), Error>;
    fn rollback(&self) -> Result<(), Error>;
    fn tip(&self) -> Result<Option<(BlockNumber, Byte32)>, Error>;
    fn prune(&self) -> Result<(), Error>;
    fn get_live_cells_by_lock_script(&self, lock_script: &Script) -> Result<Vec<OutPoint>, Error>;
    fn get_live_cells_by_type_script(&self, type_script: &Script) -> Result<Vec<OutPoint>, Error>;
    fn get_transactions_by_lock_script(&self, lock_script: &Script) -> Result<Vec<Byte32>, Error>;
    fn get_transactions_by_type_script(&self, type_script: &Script) -> Result<Vec<Byte32>, Error>;
    fn get_detailed_live_cell(
        &self,
        out_point: &OutPoint,
    ) -> Result<Option<DetailedLiveCell>, Error>;
    fn report(&self) -> Result<(), Error>;
}

#[derive(Debug)]
pub enum Error {
    StoreError(String),
}
impl From<StoreError> for Error {
    fn from(e: StoreError) -> Error {
        match e {
            StoreError::DBError(s) => Error::StoreError(s),
        }
    }
}
