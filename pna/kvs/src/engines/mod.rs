//! Different implementations of `KvsEngine`
mod kvs;
mod sled;

pub use self::sled::SledKvsEngine;
pub use kvs::KvStore;
