//! An `KvsEngine` that uses log-structure file system.

use crate::engines::{KvsEngineBackend, KVS_ENGINE_BACKEND_FILENAME};
use crate::{Error, ErrorKind, KvsEngine, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

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
/// use kvs::{KvsEngine, Result};
/// use kvs::engines::KvStore;
/// use tempfile::TempDir;
///
/// fn main() -> Result<()> {
///     let temp_dir = TempDir::new().expect("unable to create temporary working directory");
///     let mut kvs = KvStore::open(temp_dir.path())?;
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
#[derive(Debug, Clone)]
pub struct KvStore {
    context: Arc<Mutex<Context>>,
}

#[derive(Debug)]
struct Context {
    active_path: PathBuf,
    active_epoch: u64,
    garbage: u64,
    writer: BufWriter<File>,
    readers: HashMap<u64, BufReader<File>>,
    index_map: HashMap<String, KvsLogEntryIndex>,
}

impl KvStore {
    /// Open the key-value store at the given path and return the store to the caller.
    pub fn open<P>(path: P) -> Result<Self>
    where
        P: Into<PathBuf>,
    {
        let active_path = path.into();

        let backend_path = active_path.join(KVS_ENGINE_BACKEND_FILENAME);
        fs::write(backend_path, KvsEngineBackend::Kvs.as_str())?;

        let prev_epochs = previous_epochs(&active_path)?;
        let active_epoch = prev_epochs.last().map(|&e| e + 1).unwrap_or_default();

        // go through all log files, rebuild the index, and keep the handle to each log for later access
        let mut garbage = 0;
        let mut readers = HashMap::new();
        let mut index_map = HashMap::new();
        for prev_epoch in prev_epochs {
            let prev_log_path = active_path.join(format!("epoch-{}.log", prev_epoch));
            let prev_log = OpenOptions::new().read(true).open(&prev_log_path)?;
            let mut reader = BufReader::new(prev_log);

            garbage += build_index(&mut reader, &mut index_map, prev_epoch)?;
            readers.insert(prev_epoch, reader);
        }
        // create a new log file for this instance, taking a write handle and a read handle for it
        let (writer, reader) = create_log(&active_path, active_epoch)?;
        readers.insert(active_epoch, reader);

        let context = Context {
            active_path,
            active_epoch,
            garbage,
            writer,
            readers,
            index_map,
        };

        Ok(Self {
            context: Arc::new(Mutex::new(context)),
        })
    }

    fn merge(&self) -> Result<()> {
        let mut context = self.context.lock().unwrap();

        // create 2 new log: one for the merged entries and one for the new active log
        let merged_epoch = context.active_epoch + 1;
        context.active_epoch += 2;
        let (writer, reader) = create_log(&context.active_path, context.active_epoch)?;
        context.writer = writer;
        let active_epoch = context.active_epoch;
        context.readers.insert(active_epoch, reader);
        let (mut merged_writer, merged_reader) = create_log(&context.active_path, merged_epoch)?;
        context.readers.insert(merged_epoch, merged_reader);

        let mut new_index_map = context.index_map.clone();
        // copy data from old log files to the merged log file and update the in-memory index map
        for index in new_index_map.values_mut() {
            match context.readers.get_mut(&index.epoch) {
                Some(reader) => {
                    reader.seek(SeekFrom::Start(index.offset))?;
                    let mut entry_reader = reader.take(index.length);

                    let merged_offset = merged_writer.stream_position()?;
                    io::copy(&mut entry_reader, &mut merged_writer)?;

                    *index = KvsLogEntryIndex {
                        epoch: merged_epoch,
                        offset: merged_offset,
                        length: index.length,
                    };
                }
                None => {
                    return Err(Error::new(
                        ErrorKind::CorruptedIndex,
                        format!("Could not get reader for epoch #{}", index.epoch),
                    ))
                }
            }
        }
        context.index_map.clear();
        context.index_map.clone_from(&new_index_map);
        merged_writer.flush()?;

        // remove stale log files
        let stale_epochs: Vec<u64> = context
            .readers
            .keys()
            .filter(|&&epoch| epoch < merged_epoch)
            .cloned()
            .collect();
        for epoch in stale_epochs {
            let log_path = context.active_path.join(format!("epoch-{}.log", epoch));
            fs::remove_file(log_path)?;
            context.readers.remove(&epoch);
        }
        context.garbage = 0;
        Ok(())
    }
}

impl KvsEngine for KvStore {
    /// # Error
    ///
    /// Error from I/O operations and serialization/deserialization operations will be propagated.
    fn set(&self, key: String, val: String) -> Result<()> {
        /*
            When setting a value a key, a `Set` command is written to disk in a sequential log,
            then the log pointer (file offset) is stored in an in-memory index from key to pointer.
        */
        let mut context = self.context.lock().unwrap();

        let active_offset = context.writer.stream_position()?;
        let command = KvsLogEntry::Set(key.clone(), val);
        bincode::serialize_into(&mut context.writer, &command)?;
        context.writer.flush()?;

        let index = KvsLogEntryIndex {
            epoch: context.active_epoch,
            offset: active_offset,
            length: context.writer.stream_position()? - active_offset,
        };
        if let Some(prev_index) = context.index_map.insert(key, index) {
            context.garbage += prev_index.length;
            if context.garbage > GARBAGE_THRESHOLD {
                self.merge()?;
            }
        };

        Ok(())
    }

    /// Returns the value of a key, if the key exists. Otherwise, returns `None`.
    ///
    /// # Error
    ///
    /// Error from I/O operations will be propagated.
    fn get(&self, key: String) -> Result<Option<String>> {
        /*
            When retrieving a value for a key, the store searches for the key in the index. If
            found an index, loads the command from the log at the corresponding log pointer,
            evaluates the command, and returns the result.
        */
        let mut context = self.context.lock().unwrap();

        let get_result = context.index_map.get(&key).cloned();
        match get_result {
            Some(index) => match context.readers.get_mut(&index.epoch) {
                Some(reader) => {
                    reader.seek(SeekFrom::Start(index.offset))?;
                    match bincode::deserialize_from(reader)? {
                        KvsLogEntry::Set(_, value) => Ok(Some(value)),
                        _ => Err(Error::new(
                            ErrorKind::CorruptedLog,
                            "Expecting a log entry for a set operation",
                        )),
                    }
                }
                None => Err(Error::new(
                    ErrorKind::CorruptedIndex,
                    format!("Could not get reader for epoch #{}", index.epoch),
                )),
            },
            None => Ok(None),
        }
    }

    /// Removes a key.
    ///
    /// # Error
    ///
    /// Error from I/O operations will be propagated. If the key doesn't exist returns a
    /// `KeyNotFound` error.
    fn remove(&self, key: String) -> Result<()> {
        /*
            When removing a key, the store searches for the key in the index. If an index is found,
            a `Remove` command is written to disk a in sequential log, and the key is removed from
            the in-memory index.
        */
        let mut context = self.context.lock().unwrap();

        if !context.index_map.contains_key(&key) {
            return Err(Error::new(
                ErrorKind::KeyNotFound,
                format!("Key '{}' does not exist", key),
            ));
        }

        let command = KvsLogEntry::Rm(key.clone());
        bincode::serialize_into(&mut context.writer, &command)?;
        context.writer.flush()?;

        if let Some(prev_index) = context.index_map.remove(&key) {
            context.garbage += prev_index.length;
            if context.garbage > GARBAGE_THRESHOLD {
                self.merge()?;
            }
        };
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum KvsLogEntry {
    Set(String, String),
    Rm(String),
}

#[derive(Debug, Clone)]
struct KvsLogEntryIndex {
    epoch: u64,
    offset: u64,
    length: u64,
}

fn build_index(
    reader: &mut BufReader<File>,
    index_map: &mut HashMap<String, KvsLogEntryIndex>,
    epoch: u64,
) -> Result<u64> {
    reader.seek(SeekFrom::Start(0))?;
    let mut garbage = 0;
    loop {
        let offset = reader.stream_position()?;
        match bincode::deserialize_from(reader.by_ref()) {
            Ok(command) => match command {
                KvsLogEntry::Set(key, _) => {
                    let index = KvsLogEntryIndex {
                        epoch,
                        offset,
                        length: reader.stream_position()? - offset,
                    };
                    if let Some(prev_index) = index_map.insert(key, index) {
                        garbage += prev_index.length;
                    };
                }
                KvsLogEntry::Rm(key) => {
                    if let Some(prev_index) = index_map.remove(&key) {
                        garbage += prev_index.length;
                    };
                }
            },
            Err(err) => match err.as_ref() {
                bincode::ErrorKind::Io(io_err) => match io_err.kind() {
                    io::ErrorKind::UnexpectedEof => break,
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
    let mut log_path = path.into();
    log_path.push(format!("epoch-{}.log", epoch));

    let writable_log = OpenOptions::new()
        .create_new(true)
        .append(true)
        .open(&log_path)?;
    let readable_log = OpenOptions::new().read(true).open(&log_path)?;

    let writer = BufWriter::new(writable_log);
    let reader = BufReader::new(readable_log);
    Ok((writer, reader))
}

fn previous_epochs<P>(path: P) -> Result<Vec<u64>>
where
    P: AsRef<Path>,
{
    let mut epochs: Vec<u64> = fs::read_dir(path.as_ref())?
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
