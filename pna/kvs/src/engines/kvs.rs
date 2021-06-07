//! An `KvsEngine` that uses log-structure file system.

use crate::{Error, ErrorKind, KvsEngine, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
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
///     let kvs = KvStore::open(temp_dir.path())?;
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
    // NOTE: Breaking up the lock
    //
    // # Requirements
    // - Read from index and from disk on multiple threads at a time
    // - Write command to disk while maintaining the index
    // - Read in parallel with write, i.e., readers will always see a consistent state
    //   - Maintaining an invariant that the index always points to a valid command in the log
    //   - Maintaining other invariants for bookkeeping state
    // - Periodically compact disk's data, while maintaining invariants for readers
    //
    // # How to break up lock
    //
    // - Understand and maintain the program sequential consistency
    // - Identify immutable values
    // - Duplicate instead of sharing
    // - Break up data structures by role
    // - Use specialized concurrent data structure
    // - Postpone cleanup until later
    // - Share flags and counters with atomics
    context: Arc<Mutex<Context>>,
}

impl KvStore {
    /// Open the key-value store at the given path and return the store to the caller.
    pub fn open<P>(path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let prev_epochs = previous_epochs(&path)?;
        let active_epoch = prev_epochs.last().map(|&e| e + 1).unwrap_or_default();

        // go through all log files, rebuild the index, and keep the handle to each log for later access
        let mut garbage = 0;
        let mut readers = HashMap::new();
        let mut index_map = BTreeMap::new();
        for prev_epoch in prev_epochs {
            let prev_log_path = path.as_ref().join(format!("epoch-{}.log", prev_epoch));
            let prev_log = OpenOptions::new().read(true).open(prev_log_path)?;
            let mut reader = BufSeekReader::new(prev_log)?;

            garbage += build_index(&mut reader, &mut index_map, prev_epoch)?;
            readers.insert(prev_epoch, reader);
        }
        // create a new log file for this instance, taking a write handle and a read handle for it
        let (writer, reader) = create_log(&path, active_epoch)?;
        readers.insert(active_epoch, reader);

        let context = Context {
            active_path: path.as_ref().to_path_buf(),
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
}

impl KvsEngine for KvStore {
    /// # Error
    ///
    /// Error from I/O operations and serialization/deserialization operations will be propagated.
    fn set(&self, key: String, val: String) -> Result<()> {
        let mut context = self.context.lock().unwrap();
        context.set(key, val)
    }

    /// Returns the value of a key, if the key exists. Otherwise, returns `None`.
    ///
    /// # Error
    ///
    /// Error from I/O operations will be propagated.
    fn get(&self, key: String) -> Result<Option<String>> {
        let mut context = self.context.lock().unwrap();
        context.get(key)
    }

    /// Removes a key.
    ///
    /// # Error
    ///
    /// Error from I/O operations will be propagated. If the key doesn't exist returns a
    /// `KeyNotFound` error.
    fn remove(&self, key: String) -> Result<()> {
        let mut context = self.context.lock().unwrap();
        context.remove(key)
    }
}

#[derive(Debug)]
struct Context {
    active_path: PathBuf,
    active_epoch: u64,
    garbage: u64,
    writer: BufSeekWriter<File>,
    readers: HashMap<u64, BufSeekReader<File>>,
    index_map: BTreeMap<String, LogIndex>,
}

impl Context {
    fn set(&mut self, key: String, val: String) -> Result<()> {
        /*
            A `LogEntry` for `Set` command is written to a sequential log on disk, then the log
            pointer (file position) is stored in an in-memory index from key to pointer.
        */
        let log_entry = LogEntry::Set(key.clone(), val);

        let pos = self.writer.pos;
        bincode::serialize_into(&mut self.writer, &log_entry)?;
        self.writer.flush()?;
        let len = self.writer.pos - pos;

        let index = LogIndex {
            gen: self.active_epoch,
            pos,
            len,
        };
        if let Some(prev_index) = self.index_map.insert(key, index) {
            self.garbage += prev_index.len;
            if self.garbage > GARBAGE_THRESHOLD {
                self.merge()?;
            }
        };

        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        /*
            When retrieving a value for a key, the store searches for the key in the index. If
            found an index, loads the log entry at the corresponding log pointer, evaluates the
            contained command, and returns the result.
        */
        let get_result = self.index_map.get(&key).cloned();
        match get_result {
            Some(index) => match self.readers.get_mut(&index.gen) {
                Some(reader) => {
                    reader.seek(SeekFrom::Start(index.pos))?;
                    match bincode::deserialize_from(reader)? {
                        LogEntry::Set(_, value) => Ok(Some(value)),
                        _ => Err(Error::new(
                            ErrorKind::CorruptedLog,
                            "Expecting a log entry for a set operation",
                        )),
                    }
                }
                None => Err(Error::new(
                    ErrorKind::CorruptedIndex,
                    format!("Could not get reader for epoch #{}", index.gen),
                )),
            },
            None => Ok(None),
        }
    }

    fn remove(&mut self, key: String) -> Result<()> {
        /*
            When removing a key, the store searches for the key in the index. If an index is found,
            a log entry for the remove command is written to the on-disk sequential log, and the key
            is then removed from the index.
        */

        if !self.index_map.contains_key(&key) {
            return Err(Error::new(
                ErrorKind::KeyNotFound,
                format!("Key '{}' does not exist", key),
            ));
        }

        let command = LogEntry::Rm(key.clone());
        bincode::serialize_into(&mut self.writer, &command)?;
        self.writer.flush()?;

        if let Some(prev_index) = self.index_map.remove(&key) {
            self.garbage += prev_index.len;
            if self.garbage > GARBAGE_THRESHOLD {
                self.merge()?;
            }
        };
        Ok(())
    }

    fn merge(&mut self) -> Result<()> {
        // create 2 new log: one for the merged entries and one for the new active log
        let merged_epoch = self.active_epoch + 1;
        self.active_epoch += 2;
        let (writer, reader) = create_log(&self.active_path, self.active_epoch)?;
        self.writer = writer;
        let active_epoch = self.active_epoch;
        self.readers.insert(active_epoch, reader);
        let (mut merged_writer, merged_reader) = create_log(&self.active_path, merged_epoch)?;
        self.readers.insert(merged_epoch, merged_reader);

        let mut new_index_map = self.index_map.clone();
        // copy data from old log files to the merged log file and update the in-memory index map
        for index in new_index_map.values_mut() {
            match self.readers.get_mut(&index.gen) {
                Some(reader) => {
                    reader.seek(SeekFrom::Start(index.pos))?;
                    let mut entry_reader = reader.take(index.len);

                    let merged_pos = merged_writer.pos;
                    io::copy(&mut entry_reader, &mut merged_writer)?;

                    *index = LogIndex {
                        gen: merged_epoch,
                        pos: merged_pos,
                        len: index.len,
                    };
                }
                None => {
                    return Err(Error::new(
                        ErrorKind::CorruptedIndex,
                        format!("Could not get reader for epoch #{}", index.gen),
                    ))
                }
            }
        }
        self.index_map.clear();
        self.index_map.clone_from(&new_index_map);
        merged_writer.flush()?;

        // remove stale log files
        let stale_epochs: Vec<u64> = self
            .readers
            .keys()
            .filter(|&&epoch| epoch < merged_epoch)
            .cloned()
            .collect();
        for epoch in stale_epochs {
            let log_path = self.active_path.join(format!("epoch-{}.log", epoch));
            fs::remove_file(log_path)?;
            self.readers.remove(&epoch);
        }
        self.garbage = 0;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum LogEntry {
    Set(String, String),
    Rm(String),
}

#[derive(Debug, Clone)]
struct LogIndex {
    gen: u64,
    pos: u64,
    len: u64,
}

fn build_index(
    reader: &mut BufSeekReader<File>,
    index_map: &mut BTreeMap<String, LogIndex>,
    epoch: u64,
) -> Result<u64> {
    reader.seek(SeekFrom::Start(0))?;
    let mut garbage = 0;
    loop {
        let pos = reader.pos;
        match bincode::deserialize_from(reader.by_ref()) {
            Ok(command) => match command {
                LogEntry::Set(key, _) => {
                    let index = LogIndex {
                        gen: epoch,
                        pos,
                        len: reader.pos - pos,
                    };
                    if let Some(prev_index) = index_map.insert(key, index) {
                        garbage += prev_index.len;
                    };
                }
                LogEntry::Rm(key) => {
                    if let Some(prev_index) = index_map.remove(&key) {
                        garbage += prev_index.len;
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

fn create_log<P>(path: P, epoch: u64) -> Result<(BufSeekWriter<File>, BufSeekReader<File>)>
where
    P: AsRef<Path>,
{
    let log_path = path.as_ref().join(format!("epoch-{}.log", epoch));

    let writable_log = OpenOptions::new()
        .create_new(true)
        .append(true)
        .open(&log_path)?;
    let readable_log = OpenOptions::new().read(true).open(&log_path)?;

    let writer = BufSeekWriter::new(writable_log)?;
    let reader = BufSeekReader::new(readable_log)?;
    Ok((writer, reader))
}

fn previous_epochs<P>(path: P) -> Result<Vec<u64>>
where
    P: AsRef<Path>,
{
    let mut epochs: Vec<u64> = fs::read_dir(&path)?
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

#[derive(Debug)]
struct BufSeekWriter<W>
where
    W: Write,
{
    pos: u64,
    writer: BufWriter<W>,
}

impl<W> BufSeekWriter<W>
where
    W: Write,
{
    fn new(mut w: W) -> Result<Self>
    where
        W: Write + Seek,
    {
        let pos = w.seek(SeekFrom::Current(0))?;
        let writer = BufWriter::new(w);
        Ok(Self { pos, writer })
    }
}

impl<W> Write for BufSeekWriter<W>
where
    W: Write,
{
    fn write(&mut self, b: &[u8]) -> std::result::Result<usize, io::Error> {
        self.writer.write(b).and_then(|bytes_written| {
            self.pos += bytes_written as u64;
            Ok(bytes_written)
        })
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

#[derive(Debug)]
struct BufSeekReader<R>
where
    R: Read + Seek,
{
    pos: u64,
    reader: BufReader<R>,
}

impl<R> BufSeekReader<R>
where
    R: Read + Seek,
{
    fn new(mut r: R) -> Result<Self> {
        let pos = r.seek(SeekFrom::Current(0))?;
        let reader = BufReader::new(r);
        Ok(Self { pos, reader })
    }
}

impl<R> Read for BufSeekReader<R>
where
    R: Read + Seek,
{
    fn read(&mut self, b: &mut [u8]) -> std::result::Result<usize, io::Error> {
        self.reader.read(b).and_then(|bytes_read| {
            self.pos += bytes_read as u64;
            Ok(bytes_read)
        })
    }
}
impl<R> Seek for BufSeekReader<R>
where
    R: Read + Seek,
{
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.reader.seek(pos).and_then(|posn| {
            self.pos = posn;
            Ok(posn)
        })
    }
}
