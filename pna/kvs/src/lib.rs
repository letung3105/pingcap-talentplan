//! Provides simple data structures with associated methods that help with storing data in
//! key-value pairs.

#![deny(missing_docs, missing_debug_implementations)]

pub mod kvs_engine;
pub mod error;

pub use kvs_engine::KvStore;
pub use error::{Error, ErrorKind, Result};

/// Define the interface of a key-value store
pub trait KvsEngine {
    /// Sets a value to a key.
    fn set(&mut self, key: String, value: String) -> Result<()>;

    /// Returns the value of a key, if the key exists. Otherwise, returns `None`.
    fn get(&mut self, key: String) -> Result<Option<String>>;

    /// Removes a key.
    fn remove(&mut self, key: String) -> Result<()>;
}
