//! Different implementations of `KvsEngine`
mod kvs;
mod sled;

pub use self::kvs::KvStore;
pub use self::sled::SledKvsEngine;

use crate::{Error, ErrorKind, Result};
use std::fs;
use std::io;
use std::path::PathBuf;
use std::str::FromStr;

/// The file that contains the name of key-value store engine used in the directory
pub const KVS_ENGINE_BACKEND_FILENAME: &str = "ENGINE_BACKEND";
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
pub enum KvsEngineBackend {
    /// Default engine provided by the library
    Kvs,
    /// Uses the in-memory key-value store `sled`
    Sled,
}

impl KvsEngineBackend {
    /// Get the string representation of the key-value store engine backend
    pub fn as_str(&self) -> &'static str {
        match *self {
            Self::Kvs => "kvs",
            Self::Sled => "sled",
        }
    }
}

impl FromStr for KvsEngineBackend {
    type Err = Error;

    fn from_str(s: &str) -> Result<KvsEngineBackend> {
        let name = s.to_lowercase();
        match name.as_str() {
            "kvs" => Ok(Self::Kvs),
            "sled" => Ok(Self::Sled),
            _ => Err(Error::from(ErrorKind::UnsupportedKvsEngineBackend)),
        }
    }
}

/// Parse the [`KvsEngineBackend`] that was previously used in the given directory,
/// and compare that against the chosen [`KvsEngineBackend`].
/// Returns the [`KvsEngineBackend`] that will be used.
///
/// [`KvsEngineBackend`]: crate::KvsEngineBackend
pub fn choose_engine_backend<P>(
    path: P,
    engine_backend: Option<KvsEngineBackend>,
) -> Result<KvsEngineBackend>
where
    P: Into<PathBuf>,
{
    let mut engine_backend_path = path.into();
    engine_backend_path.push(KVS_ENGINE_BACKEND_FILENAME);

    match fs::read_to_string(engine_backend_path) {
        Ok(prev_engine_backend) => {
            let prev_engine_backend = KvsEngineBackend::from_str(&prev_engine_backend)?;
            let engine_backend = engine_backend.unwrap_or(prev_engine_backend);
            if engine_backend == prev_engine_backend {
                Ok(engine_backend)
            } else {
                Err(Error::from(ErrorKind::MismatchedKvsEngineBackend))
            }
        }
        Err(err) => {
            if let io::ErrorKind::NotFound = err.kind() {
                Ok(engine_backend.unwrap_or(KvsEngineBackend::Kvs))
            } else {
                Err(Error::from(err))
            }
        }
    }
}
