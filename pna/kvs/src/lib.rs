//! Provides simple data structures with associated methods that help with storing data in
//! key-value pairs.

#![deny(missing_docs, missing_debug_implementations)]

use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Seek, SeekFrom};
use std::path::Path;
use std::{collections::HashMap, io::Write};

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
/// use tempfile::TempDir;
///
/// fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let kvs_dir = std::env::current_dir()?;
/// let mut kvs = KvStore::open(kvs_dir)?;
///
/// kvs.set("key".to_string(), "val".to_string())?;
/// let val = kvs.get("key".to_string())?;
/// assert_eq!(val, Some("val".to_string()));
///
/// kvs.set("key".to_string(),"val-dirty".to_string())?;
/// let val = kvs.get("key".to_string())?;
/// assert_eq!(val, Some("val-dirty".to_string()));
///
/// kvs.remove("key".to_string())?;
/// assert_eq!(None, kvs.get("key".to_string())?);
/// if let Ok(_) = kvs.remove("key".to_string()) {
///     assert!(false);
/// }
///
/// Ok(())
/// }
/// ```
#[derive(Debug)]
pub struct KvStore {
    index: HashMap<String, String>,
    writer: BufWriter<File>,
    reader: BufReader<File>,
}

impl KvStore {
    /// Open the key-value store at the given path and return the store to the caller.
    pub fn open<P>(path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        static DEFAULT_ACTIVE_LOG_NAME: &str = "db.log";
        let log_path = path.as_ref().join(DEFAULT_ACTIVE_LOG_NAME);

        let wlog = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)?;
        let rlog = OpenOptions::new().read(true).open(&log_path)?;

        let index = HashMap::default();
        let writer = BufWriter::new(wlog);
        let reader = BufReader::new(rlog);

        Ok(Self {
            index,
            writer,
            reader,
        })
    }

    /// Set the given key to a value. An error is returned if the value is not written successfully.
    ///
    /// # Error
    ///
    /// Error from I/O operations and serialization/deserialization operations will be propagated
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        /*
            When setting a value a key, a `Set` command is written to disk in a sequential log,
            then the log pointer (file offset) is stored in an in-memory index from key to pointer.
        */
        self.append_log(KvStoreCommand::Set(key, value))
    }

    /// Get the value of the given key. If the key does not exist, return `None`. An error is
    /// returned if the key is not read successfully.
    ///
    /// # Error
    ///
    /// Error from I/O operations will be propagated
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        /*
            When retrieving a value for a key, the store searches for the key in the index. If
            found an index, loads the command from the log at the corresponding log pointer,
            evaluates the command, and returns the result.
        */
        self.build_index()?;
        Ok(self.index.get(&key).cloned())
    }

    /// Remove the given key. An error is returned if the key does not exist or if it is not
    /// removed successfully.
    ///
    /// # Error
    ///
    /// Error from I/O operations will be propagated. If the key doesn't exist returns a
    /// `KeyNotFound` error
    pub fn remove(&mut self, key: String) -> Result<()> {
        /*
            When removing a key, the store searches for the key in the index. If an index is found,
            a `Remove` command is written to disk a in sequential log, and the key is removed from
            the in-memory index.
        */
        self.build_index()?;
        if !self.index.contains_key(&key) {
            return Err(KvStoreError::KeyNotFound);
        }
        self.append_log(KvStoreCommand::Rm(key))
    }

    fn build_index(&mut self) -> Result<()> {
        let file_size = self.reader.get_ref().metadata()?.len();

        self.reader.seek(SeekFrom::Start(0))?;
        while self.reader.stream_position()? < file_size {
            let cmd: KvStoreCommand = bincode::deserialize_from(&mut self.reader)?;
            match cmd {
                KvStoreCommand::Set(key, val) => {
                    self.index.insert(key, val);
                }
                KvStoreCommand::Rm(key) => {
                    self.index.remove(&key);
                }
                _ => {}
            }
        }

        Ok(())
    }

    fn append_log(&mut self, command: KvStoreCommand) -> Result<()> {
        bincode::serialize_into(&mut self.writer, &command)?;
        // TODO: only flush when needed
        self.writer.flush()?;
        Ok(())
    }
}

/// Data structure for possible operations on the key-value store
#[derive(Debug, Serialize, Deserialize)]
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
    /// Error when performing operations on non-existent key
    KeyNotFound,

    /// Error from I/O operations
    IoError(std::io::Error),

    /// Error from serialization/deserialization operations
    Bincode(bincode::Error),
}

impl std::error::Error for KvStoreError {}

impl std::fmt::Display for KvStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::KeyNotFound => write!(f, "Key not found"),
            Self::IoError(err) => write!(f, "I/O error {}", err),
            Self::Bincode(err) => write!(f, "Serialize/Deserialize error {}", err),
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
