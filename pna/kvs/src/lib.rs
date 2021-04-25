//! Provides simple data structures with associated methods that help with storing data in
//! key-value pairs.

#![deny(missing_docs, missing_debug_implementations)]

pub mod error;
pub use error::{Error, ErrorKind, Result};

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

/// A simple key-value that has supports for inserting, updating, accessing, and removing entries.
/// This implementation holds that key-value inside the main memory that doesn't support data
/// persistence.
///
/// Serialization/Deserialization is done using the `bincode` crate. The crate is chosen mainly
/// because of its performance.
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
    epoch: u64,
    index: HashMap<String, CommandIndex>,
    writer: BufWriter<File>,
    readers: HashMap<u64, BufReader<File>>,
}

impl KvStore {
    /// Open the key-value store at the given path and return the store to the caller.
    pub fn open<P>(path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let epochs = Self::get_previous_epochs(&path)?;
        let epoch_current = epochs.last().map(|e| *e + 1).unwrap_or_default();

        // go through all log files and rebuild the index
        let mut index = HashMap::default();
        let mut readers = HashMap::default();
        for epoch in epochs {
            let log_path = path.as_ref().join(format!("epoch-{}.log", epoch));
            let rlog = OpenOptions::new().read(true).open(&log_path)?;
            let mut reader = BufReader::new(rlog);

            Self::build_index(&mut reader, &mut index, epoch)?;
            readers.insert(epoch, reader);
        }
        // create a new log file for this instance
        let (mut writer, reader) = Self::new_log(&path, epoch_current)?;
        writer.seek(SeekFrom::End(0))?;
        readers.insert(epoch_current, reader);

        Ok(Self {
            epoch: epoch_current,
            index,
            writer,
            readers,
        })
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
        match self.index.get(&key) {
            Some(CommandIndex { epoch, offset }) => match self.readers.get_mut(epoch) {
                Some(reader) => {
                    reader.seek(SeekFrom::Start(*offset))?;
                    match bincode::deserialize_from(reader)? {
                        Command::Set(_, val) => Ok(Some(val)),
                        _ => Err(Error::new(ErrorKind::InvalidCommand)),
                    }
                }
                None => Err(Error::new(ErrorKind::InvalidReaderEpoch)),
            },
            None => Ok(None),
        }
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
        let epoch = self.epoch;
        let offset = self.writer.stream_position()?;
        bincode::serialize_into(&mut self.writer, &Command::Set(key.clone(), val))?;
        self.writer.flush()?;
        self.index.insert(key, CommandIndex { epoch, offset });
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
        let cmd = Command::Rm(key.clone());
        bincode::serialize_into(&mut self.writer, &cmd)?;
        self.writer.flush()?;
        self.index.remove(&key);
        Ok(())
    }

    fn build_index(
        reader: &mut BufReader<File>,
        index: &mut HashMap<String, CommandIndex>,
        epoch: u64,
    ) -> Result<()> {
        reader.seek(SeekFrom::Start(0))?;
        loop {
            let offset = reader.stream_position()?;
            bincode::deserialize_from(reader.by_ref())
                .map(|cmd| match cmd {
                    Command::Set(key, _) => {
                        index.insert(key, CommandIndex { epoch, offset });
                    }
                    Command::Rm(key) => {
                        index.remove(&key);
                    }
                })
                .or_else(|err| match err.as_ref() {
                    bincode::ErrorKind::Io(io_err) => match io_err.kind() {
                        std::io::ErrorKind::UnexpectedEof => Ok(()),
                        _ => Err(Error::from(err)),
                    },
                    _ => Err(Error::from(err)),
                })?;
        }
    }

    fn new_log<P>(path: P, epoch: u64) -> Result<(BufWriter<File>, BufReader<File>)>
    where
        P: AsRef<Path>,
    {
        let log_path = path.as_ref().join(format!("epoch-{}.log", epoch));
        let wlog = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)?;
        let rlog = OpenOptions::new().read(true).open(&log_path)?;
        let writer = BufWriter::new(wlog);
        let reader = BufReader::new(rlog);
        Ok((writer, reader))
    }

    fn get_previous_epochs<P>(path: P) -> Result<Vec<u64>>
    where
        P: AsRef<Path>,
    {
        let mut epochs: Vec<u64> = std::fs::read_dir(path.as_ref())?
            .filter_map(std::result::Result::ok)
            .map(|e| e.path())
            .filter(|p| p.is_file() && p.extension() == Some("log".as_ref()))
            .filter_map(|p| {
                p.file_stem()
                    .and_then(OsStr::to_str)
                    .filter(|s| s.starts_with("epoch-"))
                    .map(|s| s.trim_start_matches("epoch-"))
                    .map(str::parse::<u64>)
            })
            .filter_map(std::result::Result::ok)
            .collect();
        epochs.sort();
        Ok(epochs)
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum Command {
    Set(String, String),
    Rm(String),
}

#[derive(Debug)]
struct CommandIndex {
    epoch: u64,
    offset: u64,
}
