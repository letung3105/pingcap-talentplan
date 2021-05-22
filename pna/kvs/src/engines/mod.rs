//! Different implementations of `KvsEngine`
mod kvs;
mod sled;

pub use self::sled::SledKvsEngine;
pub use kvs::KvStore;

/// The file that contains the name of key-value store engine used in the directory
pub const KVS_ENGINE_VARIANT_FILE: &str = "KVS_ENGINE_VARIANT";
