//! Different implementations of `KvsEngine`
mod kvs;
mod sled;

pub use self::kvs::KvStore;
pub use self::sled::SledKvsEngine;

use crate::{Error, ErrorKind, Result};
use std::str::FromStr;

/// Define the interface of a key-value store
pub trait KvsEngine: Clone + Send + 'static {
    /// Sets a value to a key.
    fn set(&self, key: String, value: String) -> Result<()>;

    /// Returns the value of a key, if the key exists. Otherwise, returns `None`.
    fn get(&self, key: String) -> Result<Option<String>>;

    /// Removes a key.
    fn remove(&self, key: String) -> Result<()>;
}

/// Different engines that can be used for the key-value store
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Engine {
    /// Default engine provided by the library
    Kvs,
    /// Uses the in-memory key-value store `sled`
    Sled,
}

impl Engine {
    /// Get the string representation of the key-value store engine backend
    pub fn as_str(&self) -> &'static str {
        match *self {
            Self::Kvs => "kvs",
            Self::Sled => "sled",
        }
    }
}

impl FromStr for Engine {
    type Err = Error;

    fn from_str(s: &str) -> Result<Engine> {
        let name = s.to_lowercase();
        match name.as_str() {
            "kvs" => Ok(Self::Kvs),
            "sled" => Ok(Self::Sled),
            _ => Err(Error::new(
                ErrorKind::UnsupportedKvsEngineBackend,
                format!("Could not found engine named '{}'", name),
            )),
        }
    }
}
