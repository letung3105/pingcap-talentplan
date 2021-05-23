//! Provides simple data structures with associated methods that help with storing data in
//! key-value pairs.

#![deny(missing_docs, missing_debug_implementations)]

// TODO: add logging library

pub mod engines;
pub mod error;
pub mod proto;

pub use engines::{open, KvStore, SledKvsEngine};
pub use error::{Error, ErrorKind, Result};
pub use proto::{KvsClient, KvsServer};

use std::str::FromStr;

/// Define the interface of a key-value store
pub trait KvsEngine {
    /// Sets a value to a key.
    fn set(&mut self, key: String, value: String) -> Result<()>;

    /// Returns the value of a key, if the key exists. Otherwise, returns `None`.
    fn get(&mut self, key: String) -> Result<Option<String>>;

    /// Removes a key.
    fn remove(&mut self, key: String) -> Result<()>;
}

impl std::fmt::Debug for dyn KvsEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Key-value store engine")
    }
}

/// Different engines that can be used for the key-value store
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KvsEngineVariant {
    /// Default engine provided by the library
    Kvs,
    /// Uses the in-memory key-value store `sled`
    Sled,
}

impl KvsEngineVariant {
    /// Get the string representation of the key-value store engine variant
    pub fn as_str(&self) -> &'static str {
        match *self {
            Self::Kvs => "kvs",
            Self::Sled => "sled",
        }
    }
}

impl FromStr for KvsEngineVariant {
    type Err = Error;

    fn from_str(s: &str) -> Result<KvsEngineVariant> {
        let name = s.to_lowercase();
        match name.as_str() {
            "kvs" => Ok(Self::Kvs),
            "sled" => Ok(Self::Sled),
            _ => Err(Error::from(ErrorKind::UnsupportedKvsEngine)),
        }
    }
}
