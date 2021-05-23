//! Different implementations of `KvsEngine`
mod kvs;
mod sled;

pub use self::sled::SledKvsEngine;
pub use kvs::KvStore;

use std::fs;
use std::path::PathBuf;
use std::str::FromStr;

use crate::{Error, ErrorKind, KvsEngine, KvsEngineVariant, Result};

/// The file that contains the name of key-value store engine used in the directory
pub const KVS_ENGINE_VARIANT_FILE: &str = "KVS_ENGINE_VARIANT";

/// Create a new object that implements the trait `KvsEngine`
pub fn open<P>(path: P, variant: Option<KvsEngineVariant>) -> Result<Box<dyn KvsEngine>>
where
    P: Into<PathBuf>,
{
    let path = path.into();

    // check the directory for the previously used key-value store engine, an error is returned if
    // previously used engine is different than the provided one
    let is_dir_empty = fs::read_dir(&path)?.next().is_none();
    let variant = if is_dir_empty {
        variant.unwrap_or(KvsEngineVariant::Kvs)
    } else {
        let kvs_variant_path = path.join(KVS_ENGINE_VARIANT_FILE);
        let prev_variant_str = fs::read_to_string(kvs_variant_path)?;
        let prev_variant = KvsEngineVariant::from_str(&prev_variant_str)?;

        let variant = variant.unwrap_or(prev_variant);
        if variant != prev_variant {
            return Err(Error::from(ErrorKind::MismatchedKvsEngine));
        }
        variant
    };

    let kvs_engine: Box<dyn KvsEngine> = match variant {
        KvsEngineVariant::Kvs => Box::new(KvStore::open(path)?),
        KvsEngineVariant::Sled => Box::new(SledKvsEngine::open(path)?),
    };
    Ok(kvs_engine)
}
