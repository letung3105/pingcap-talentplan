//! Different implementations of `KvsEngine`
mod kvs;
mod sled;

pub use self::kvs::KvStore;
pub use self::sled::SledKvsEngine;

use crate::{KvsEngine, KvsEngineBackend, Result};
use std::path::PathBuf;

/// Create a new object that implements the trait `KvsEngine`
pub fn open<P>(path: P, backend: KvsEngineBackend) -> Result<Box<dyn KvsEngine>>
where
    P: Into<PathBuf>,
{
    let kvs_engine: Box<dyn KvsEngine> = match backend {
        KvsEngineBackend::Kvs => Box::new(KvStore::open(path)?),
        KvsEngineBackend::Sled => Box::new(SledKvsEngine::open(path)?),
    };
    Ok(kvs_engine)
}
