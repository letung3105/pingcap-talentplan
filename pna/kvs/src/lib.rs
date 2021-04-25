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
use std::path::{Path, PathBuf};

const GARBAGE_THRESHOLD: u64 = 4 * 1024 * 1024;

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
    path: PathBuf,
    epoch: u64,
    garbage: u64,
    index: HashMap<String, CommandIndex>,
    writer: BufWriter<File>,
    readers: HashMap<u64, BufReader<File>>,
}

impl KvStore {
    /// Open the key-value store at the given path and return the store to the caller.
    pub fn open<P>(path: P) -> Result<Self>
    where
        P: Into<PathBuf>,
    {
        let path = path.into();
        let epochs = Self::previous_epochs(&path)?;
        let epoch = epochs.last().map(|e| *e + 1).unwrap_or_default();

        // go through all log files and rebuild the index
        let mut garbage = 0;
        let mut index = HashMap::default();
        let mut readers = HashMap::default();
        for epoch in epochs {
            let log_path = path.join(format!("epoch-{}.log", epoch));
            let rlog = OpenOptions::new().read(true).open(&log_path)?;
            let mut reader = BufReader::new(rlog);

            garbage += Self::build_index(&mut reader, &mut index, epoch)?;
            readers.insert(epoch, reader);
        }
        // create a new log file for this instance
        let (writer, reader) = Self::create_log(&path, epoch)?;
        readers.insert(epoch, reader);

        Ok(Self {
            path,
            epoch,
            garbage,
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
            Some(cmd_idx) => match self.readers.get_mut(&cmd_idx.epoch) {
                Some(reader) => {
                    reader.seek(SeekFrom::Start(cmd_idx.offset))?;
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
        let length = self.writer.stream_position()? - offset;

        let cmd_idx = CommandIndex {
            epoch,
            offset,
            length,
        };
        if let Some(prev_cmd_idx) = self.index.insert(key, cmd_idx) {
            self.garbage += prev_cmd_idx.length;
            if self.garbage > GARBAGE_THRESHOLD {
                self.merge()?;
            }
        };
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
        if let Some(prev_cmd_idx) = self.index.remove(&key) {
            self.garbage += prev_cmd_idx.length;
            if self.garbage > GARBAGE_THRESHOLD {
                self.merge()?;
            }
        };
        Ok(())
    }

    fn merge(&mut self) -> Result<()> {
        let mut stale_epochs: Vec<u64> = self.readers.keys().cloned().collect();
        stale_epochs.sort();

        let merge_epoch = stale_epochs.last().map(|e| *e + 1).unwrap_or_default();
        let epoch = merge_epoch + 1;

        let (writer, reader) = Self::create_log(&self.path, epoch)?;
        self.writer = writer;
        self.readers.insert(epoch, reader);
        self.epoch = epoch;

        let (mut merge_writer, merge_reader) = Self::create_log(&self.path, merge_epoch)?;
        self.readers.insert(merge_epoch, merge_reader);

        for cmd_idx in self.index.values_mut() {
            match self.readers.get_mut(&cmd_idx.epoch) {
                Some(reader) => {
                    reader.seek(SeekFrom::Start(cmd_idx.offset))?;
                    let mut entry_reader = reader.take(cmd_idx.length);
                    let merge_offset = merge_writer.stream_position()?;
                    std::io::copy(&mut entry_reader, &mut merge_writer)?;
                    *cmd_idx = CommandIndex {
                        epoch: merge_epoch,
                        offset: merge_offset,
                        length: cmd_idx.length,
                    };
                }
                None => return Err(Error::new(ErrorKind::InvalidReaderEpoch)),
            }
        }
        merge_writer.flush()?;

        for epoch in stale_epochs {
            let log_path = self.path.join(format!("epoch-{}.log", epoch));
            std::fs::remove_file(log_path)?;
            self.readers.remove(&epoch);
        }
        self.garbage = 0;
        Ok(())
    }

    fn build_index(
        reader: &mut BufReader<File>,
        index: &mut HashMap<String, CommandIndex>,
        epoch: u64,
    ) -> Result<u64> {
        reader.seek(SeekFrom::Start(0))?;
        let mut garbage = 0;
        loop {
            let offset = reader.stream_position()?;
            match bincode::deserialize_from(reader.by_ref()) {
                Ok(cmd) => match cmd {
                    Command::Set(key, _) => {
                        let length = reader.stream_position()? - offset;
                        let cmd_idx = CommandIndex {
                            epoch,
                            offset,
                            length,
                        };
                        if let Some(prev_cmd_idx) = index.insert(key, cmd_idx) {
                            garbage += prev_cmd_idx.length;
                        };
                    }
                    Command::Rm(key) => {
                        if let Some(prev_cmd_idx) = index.remove(&key) {
                            garbage += prev_cmd_idx.length;
                        };
                    }
                },
                Err(err) => match err.as_ref() {
                    bincode::ErrorKind::Io(io_err) => match io_err.kind() {
                        std::io::ErrorKind::UnexpectedEof => break,
                        _ => return Err(Error::from(err)),
                    },
                    _ => return Err(Error::from(err)),
                },
            }
        }
        Ok(garbage)
    }

    fn create_log<P>(path: P, epoch: u64) -> Result<(BufWriter<File>, BufReader<File>)>
    where
        P: Into<PathBuf>,
    {
        let path = path.into();
        let log_path = path.join(format!("epoch-{}.log", epoch));
        let wlog = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(&log_path)?;
        let rlog = OpenOptions::new().read(true).open(&log_path)?;
        let writer = BufWriter::new(wlog);
        let reader = BufReader::new(rlog);
        Ok((writer, reader))
    }

    fn previous_epochs<P>(path: P) -> Result<Vec<u64>>
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
    length: u64,
}
