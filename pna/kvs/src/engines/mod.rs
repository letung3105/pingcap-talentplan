//! Different implementations of `KvsEngine`
mod kvs;
mod sled;

pub use kvs::KvStore;
pub use self::sled::SledKvsEngine;
