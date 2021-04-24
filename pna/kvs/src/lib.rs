//! Provides simple data structures with associated methods that help with storing data in
//! key-value pairs.

#![deny(missing_docs, missing_debug_implementations)]

pub mod error;
pub use error::{Error, ErrorKind, Result};

use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Seek, SeekFrom};
use std::path::Path;
use std::{collections::HashMap, io::Write};

/// A simple key-value that has supports for inserting, updating, accessing, and removing entries.
/// This implementation holds that key-value inside the main memory that doesn't support data
/// persistence.
///
/// # Usages
///
/// ```
/// use kvs::{KvStore, Result};
/// use tempfile::TempDir;
///
/// fn main() -> Result<()> {
///     let kvs_dir = std::env::current_dir()?;
///     let mut kvs = KvStore::open(kvs_dir)?;
///
///     kvs.set("key".to_string(), "val".to_string())?;
///     let val = kvs.get("key".to_string())?;
///     assert_eq!(val, Some("val".to_string()));
///
///     kvs.set("key".to_string(),"val-dirty".to_string())?;
///     let val = kvs.get("key".to_string())?;
///     assert_eq!(val, Some("val-dirty".to_string()));
///
///     kvs.remove("key".to_string())?;
///     assert_eq!(None, kvs.get("key".to_string())?);
///     if let Ok(_) = kvs.remove("key".to_string()) {
///         assert!(false);
///     }
///
///     Ok(())
/// }
/// ```
#[derive(Debug)]
pub struct KvStore {
    index: HashMap<String, CommandIndex>,
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

        let mut kvs = Self {
            index,
            writer,
            reader,
        };
        kvs.reader.seek(SeekFrom::Start(0))?;
        kvs.writer.seek(SeekFrom::End(0))?;
        kvs.build_index()?;

        Ok(kvs)
    }

    /// Returns the value of a key, if the key exists. Otherwise, returns `None`.
    ///
    /// # Error
    ///
    /// Error from I/O operations will be propagated.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        /*
            When retrieving a value for a key, the store searches for the key in the index. If
            found an index, loads the command from the log at the corresponding log pointer,
            evaluates the command, and returns the result.
        */
        if let Some(CommandIndex { pos }) = self.index.get(&key) {
            self.reader.seek(SeekFrom::Start(*pos))?;
            if let Command::Set(_, val) = bincode::deserialize_from(&mut self.reader)? {
                return Ok(Some(val));
            }
        }
        Ok(None)
    }

    /// Set a value to a key.
    ///
    /// # Error
    ///
    /// Error from I/O operations and serialization/deserialization operations will be propagated.
    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        /*
            When setting a value a key, a `Set` command is written to disk in a sequential log,
            then the log pointer (file offset) is stored in an in-memory index from key to pointer.
        */
        let pos = self.writer.stream_position()?;
        bincode::serialize_into(&mut self.writer, &Command::Set(key.clone(), val.clone()))?;
        self.writer.flush()?;
        self.index.insert(key, CommandIndex { pos });
        Ok(())
    }

    /// Removes a key.
    ///
    /// # Error
    ///
    /// Error from I/O operations will be propagated. If the key doesn't exist returns a
    /// `KeyNotFound` error.
    pub fn remove(&mut self, key: String) -> Result<()> {
        /*
            When removing a key, the store searches for the key in the index. If an index is found,
            a `Remove` command is written to disk a in sequential log, and the key is removed from
            the in-memory index.
        */
        if !self.index.contains_key(&key) {
            return Err(Error::new(ErrorKind::KeyNotFound));
        }

        bincode::serialize_into(&mut self.writer, &Command::Rm(key.clone()))?;
        self.writer.flush()?;
        self.index.remove(&key);
        Ok(())
    }

    fn build_index(&mut self) -> Result<()> {
        loop {
            let pos = self.reader.stream_position()?;
            match bincode::deserialize_from(&mut self.reader) {
                Ok(cmd) => match cmd {
                    Command::Set(key, _) => {
                        self.index.insert(key, CommandIndex { pos });
                    }
                    Command::Rm(key) => {
                        self.index.remove(&key);
                    }
                },
                Err(err) => {
                    if let bincode::ErrorKind::Io(io_err) = err.as_ref() {
                        if let std::io::ErrorKind::UnexpectedEof = io_err.kind() {
                            break;
                        }
                    }
                    return Err(Error::from(err));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum Command {
    Set(String, String),
    Rm(String),
}

#[derive(Debug)]
struct CommandIndex {
    pos: u64,
}
