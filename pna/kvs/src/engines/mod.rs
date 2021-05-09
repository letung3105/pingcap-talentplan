//! Different implementations of `KvsEngine`
pub mod kvs;
pub mod sled_kvs;

pub use sled_kvs::SledKvsEngine;
pub use kvs::KvStore;