//! Provides simple data structures with associated methods that help with storing data in
//! key-value pairs.

#![deny(missing_docs, missing_debug_implementations)]

use serde::Serialize;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::path::Path;

/// A short-hand for `std::result::Result<T, KvStoreError>`.
pub type Result<T> = std::result::Result<T, Error>;
/// A short-hand for `kvs::KvStoreError`.
pub type Error = KvStoreError;

/// A simple key-value that has supports for inserting, updating, accessing, and removing entries.
/// This implementation holds that key-value inside the main memory that doesn't support data
/// persistence.
///
/// # Example
///
/// ```rust
/// use kvs::KvStore;
///
/// fn main() -> Result<(), Box<dyn std::error::Error>> {
///     // populating the store
///     let mut kvs = KvStore::new();
///     kvs.set("key01".to_string(), "val01".to_string());
///     kvs.set("key02".to_string(), "val02".to_string());
///
///     // accessing entries
///     assert_eq!(Some("val01".to_string()), kvs.get("key01".to_string()));
///     assert_eq!(Some("val02".to_string()), kvs.get("key02".to_string()));
///     assert_eq!(None, kvs.get("key03".to_string()));
///
///     // change entry' value
///     kvs.set("key02".to_string(), "val02-dirty".to_string());
///     assert_eq!(Some("val02-dirty".to_string()), kvs.get("key02".to_string()));
///
///     // remove entry
///     kvs.remove("key02".to_string());
///     kvs.remove("key03".to_string()); // no error if key doesn't exist
///     assert_eq!(None, kvs.get("key02".to_string()));
///
///     Ok(())
/// }
/// ```
#[derive(Debug)]
pub struct KvStore {
    index: HashMap<String, String>,
    log: File,
}

impl KvStore {
    /// Open the key-value store that is located at the given path and return the store to the caller.
    pub fn open<P>(path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let index = HashMap::default();
        let log = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;
        Ok(Self { index, log })
    }

    /// Set the given key to a value. An error is returned if the value is not written successfully.
    ///
    /// # Behavior
    ///
    /// When a value is set to the key, a `Set` command is written to disk in a sequential log,
    /// then the log pointer (file offset) is stored in an in-memory index from key to pointer.
    /// The following describes the steps that will be taken.
    ///
    /// 1. Use a data structure to represent the command
    /// 2. Serialize the command
    /// 3. Append the serialized command to the file containing the log
    ///
    /// # Error
    ///
    /// + Errors of kind `bincode::Error` will be returned if the command can not be serialized
    /// + Errors of kind `std::io::Error` will be returned if error occurs while performing the operation
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = KvStoreCommand::Set(key, value);
        self.append_log(&cmd)
    }

    /// Get the value of the given key. If the key does not exist, return `None`. An error is returned if the key is
    /// not read successfully.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        todo!()
    }

    /// Remove the given key. An error is returned if the key does not exist or if it is not removed successfully.
    ///
    /// When removing a key, a `Remove` command is written to disk a in sequential log, the removes the the key from
    /// the in-memory index.
    pub fn remove(&mut self, key: String) -> Result<()> {
        todo!()
    }

    fn append_log(&mut self, command: &KvStoreCommand) -> Result<()> {
        bincode::serialize_into(&mut self.log, command)?;
        Ok(())
    }
}

/// Data structure for possible operations on the key-value store
#[derive(Debug, Serialize)]
pub enum KvStoreCommand {
    /// On-disk representation of a set command
    Set(String, String),

    /// On-disk representation of a get command
    Get(String),

    /// On-disk representation of a remove command
    Rm(String),
}

/// Error type for operations on the key-value store.
#[derive(Debug)]
pub enum KvStoreError {
    /// Error from I/O operations
    IoError(std::io::Error),

    /// Error from serialization/deserialization operations
    Bincode(bincode::Error),

    /// Error when performing operations on non-existent key
    KeyNotFound(String),
}

impl std::error::Error for KvStoreError {}

impl std::fmt::Display for KvStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IoError(err) => {
                write!(
                    f,
                    "Error encountered while performing I/O operations - {}",
                    err
                )
            }
            Self::Bincode(err) => {
                write!(
                    f,
                    "Error encountered while serializing/deserializing data - {}",
                    err
                )
            }
            Self::KeyNotFound(key) => {
                write!(f, "Key not found - '{}'", key)
            }
        }
    }
}

impl From<std::io::Error> for KvStoreError {
    fn from(err: std::io::Error) -> Self {
        Self::IoError(err)
    }
}

impl From<bincode::Error> for KvStoreError {
    fn from(err: bincode::Error) -> Self {
        Self::Bincode(err)
    }
}
