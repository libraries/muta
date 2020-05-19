use std::cell::RefCell;
use std::error::Error;
use std::path::Path;
use std::rc::Rc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use byteorder::{BigEndian, ByteOrder};
use derive_more::{Display, From};
use rocksdb::{BlockBasedOptions, ColumnFamily, Options, WriteBatch, DB};

use protocol::codec::ProtocolCodec;
use protocol::traits::{StorageAdapter, StorageBatchModify, StorageCategory, StorageSchema};
use protocol::Bytes;
use protocol::{ProtocolError, ProtocolErrorKind, ProtocolResult};

pub struct Config {
    pub options:             Options,
    pub block_based_options: BlockBasedOptions,
}

impl Config {
    pub fn default() -> Self {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_max_open_files(256);
        Self {
            options:             opts,
            block_based_options: BlockBasedOptions::default(),
        }
    }

    pub fn suggest() -> Self {
        let mut cfgs = Config::default();
        // https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning#other-general-options
        cfgs.options.set_max_background_compactions(4);
        cfgs.options.set_max_background_flushes(2);
        cfgs.options.set_bytes_per_sync(1_048_576);
        cfgs.block_based_options.set_block_size(16 * 1024);
        cfgs.block_based_options
            .set_cache_index_and_filter_blocks(true);

        // https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning#block-cache-size
        // We recommend that this should be about 1/3 of your total memory budget.
        // cfgs.block_based_options.set_lru_cache(512 << 20);

        // [TODO] https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning#bloom-filters
        // Since did not make a good decision.

        cfgs
    }
}

#[derive(Debug)]
pub struct RocksAdapter {
    pub db: Arc<DB>,
    i:  AtomicU32,
    g:  AtomicU32,
}

impl RocksAdapter {
    pub fn new<P: AsRef<Path>>(path: P, cfgs: Config) -> ProtocolResult<Self> {
        let mut opts = cfgs.options;
        opts.set_block_based_table_factory(&cfgs.block_based_options);

        let categories = [
            map_category(StorageCategory::Block),
            map_category(StorageCategory::Receipt),
            map_category(StorageCategory::SignedTransaction),
            map_category(StorageCategory::Wal),
        ];

        let db = DB::open_cf(&opts, path, categories.iter()).map_err(RocksAdapterError::from)?;

        Ok(RocksAdapter {
            db: Arc::new(db),
            i:  AtomicU32::new(0),
            g:  AtomicU32::new(0),
        })
    }
}

macro_rules! db {
    ($db:expr, $op:ident, $column:expr, $key:expr) => {
        $db.$op($column, $key).map_err(RocksAdapterError::from)
    };
    ($db:expr, $op:ident, $column:expr, $key:expr, $val:expr) => {
        $db.$op($column, $key, $val)
            .map_err(RocksAdapterError::from)
    };
}

#[async_trait]
impl StorageAdapter for RocksAdapter {
    async fn insert<S: StorageSchema>(
        &self,
        mut key: <S as StorageSchema>::Key,
        mut val: <S as StorageSchema>::Value,
    ) -> ProtocolResult<()> {
        let column = get_column::<S>(&self.db)?;
        let key = key.encode().await?.to_vec();
        let val = val.encode().await?.to_vec();

        db!(self.db, put_cf, column, key, val)?;

        Ok(())
    }

    async fn get<S: StorageSchema>(
        &self,
        mut key: <S as StorageSchema>::Key,
    ) -> ProtocolResult<Option<<S as StorageSchema>::Value>> {
        let column = get_column::<S>(&self.db)?;
        let key = key.encode().await?;

        let mut buf = [0; 4];
        BigEndian::write_u32(&mut buf, self.g.load(Ordering::SeqCst));
        let mut real_key: Vec<u8> = Vec::new();
        if self.g.load(Ordering::SeqCst) < 5000 {
            real_key.push(98);
        } else {
            real_key.push(97);
        }
        for i in 0..4 {
            real_key.push(buf[i]);
        }
        let k: &[u8] = &key[..];
        for i in 0..k.len() {
            real_key.push(k[i]);
        }

        let opt_bytes = {
            db!(self.db, get_cf, column, Bytes::from(real_key))?
                .map(|db_vec| Bytes::from(db_vec.to_vec()))
        };

        self.g.fetch_add(1, Ordering::SeqCst);

        if let Some(bytes) = opt_bytes {
            let val = <_>::decode(bytes).await?;
            Ok(Some(val))
        } else {
            Ok(None)
        }
    }

    async fn remove<S: StorageSchema>(
        &self,
        mut key: <S as StorageSchema>::Key,
    ) -> ProtocolResult<()> {
        let column = get_column::<S>(&self.db)?;
        let key = key.encode().await?.to_vec();

        db!(self.db, delete_cf, column, key)?;

        Ok(())
    }

    async fn contains<S: StorageSchema>(
        &self,
        mut key: <S as StorageSchema>::Key,
    ) -> ProtocolResult<bool> {
        let column = get_column::<S>(&self.db)?;
        let key = key.encode().await?.to_vec();
        let val = db!(self.db, get_cf, column, key)?;

        Ok(val.is_some())
    }

    async fn batch_modify<S: StorageSchema>(
        &self,
        keys: Vec<<S as StorageSchema>::Key>,
        vals: Vec<StorageBatchModify<S>>,
    ) -> ProtocolResult<()> {
        if keys.len() != vals.len() {
            return Err(RocksAdapterError::BatchLengthMismatch.into());
        }

        let column = get_column::<S>(&self.db)?;
        let mut pairs: Vec<(Bytes, Option<Bytes>)> = Vec::with_capacity(keys.len());

        let mut prefix_with_b = 0;
        for (mut key, value) in keys.into_iter().zip(vals.into_iter()) {
            let key = key.encode().await?;

            let value = match value {
                StorageBatchModify::Insert(mut value) => Some(value.encode().await?),
                StorageBatchModify::Remove => None,
            };

            let mut buf = [0; 4];
            BigEndian::write_u32(&mut buf, self.i.load(Ordering::SeqCst));
            let mut real_key: Vec<u8> = Vec::new();
            if self.i.load(Ordering::SeqCst) < 5000 {
                prefix_with_b += 1;
                real_key.push(98);
            } else {
                real_key.push(97);
            }
            for i in 0..4 {
                real_key.push(buf[i]);
            }
            let k: &[u8] = &key[..];
            for i in 0..k.len() {
                real_key.push(k[i]);
            }
            self.i.fetch_add(1, Ordering::SeqCst);

            pairs.push((Bytes::from(real_key), value))
        }
        println!("prefix_with_b {:?}", prefix_with_b);

        let mut batch = WriteBatch::default();
        for (key, value) in pairs.into_iter() {
            match value {
                Some(value) => db!(batch, put_cf, column, key, value)?,
                None => db!(batch, delete_cf, column, key)?,
            }
        }

        self.db.write(batch).map_err(RocksAdapterError::from)?;
        Ok(())
    }
}

#[derive(Debug, Display, From)]
pub enum RocksAdapterError {
    #[display(fmt = "category {} not found", _0)]
    CategoryNotFound(&'static str),

    #[display(fmt = "rocksdb {}", _0)]
    RocksDB(rocksdb::Error),

    #[display(fmt = "parameters do not match")]
    InsertParameter,

    #[display(fmt = "batch length dont match")]
    BatchLengthMismatch,
}

impl Error for RocksAdapterError {}

impl From<RocksAdapterError> for ProtocolError {
    fn from(err: RocksAdapterError) -> ProtocolError {
        ProtocolError::new(ProtocolErrorKind::Storage, Box::new(err))
    }
}

const C_BLOCKS: &str = "c1";
const C_SIGNED_TRANSACTIONS: &str = "c2";
const C_RECEIPTS: &str = "c3";
const C_WALS: &str = "c4";

fn map_category(c: StorageCategory) -> &'static str {
    match c {
        StorageCategory::Block => C_BLOCKS,
        StorageCategory::Receipt => C_RECEIPTS,
        StorageCategory::SignedTransaction => C_SIGNED_TRANSACTIONS,
        StorageCategory::Wal => C_WALS,
    }
}

fn get_column<S: StorageSchema>(db: &DB) -> Result<ColumnFamily, RocksAdapterError> {
    let category = map_category(S::category());

    let column = db
        .cf_handle(category)
        .ok_or_else(|| RocksAdapterError::from(category))?;

    Ok(column)
}
