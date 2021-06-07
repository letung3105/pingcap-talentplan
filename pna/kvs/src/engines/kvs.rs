//! An `KvsEngine` that uses log-structure file system.

use crate::{Error, ErrorKind, KvsEngine, Result};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::BTreeMap;

use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::RwLock;
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
#[derive(Debug)]
pub struct KvStore {
    // NOTE: Breaking up the lock
    //
    // # Requirements
    // - Read from index and from disk on multiple threads at a time
    // - Write log to disk while maintaining the index
    // - Read in parallel with write, i.e., readers will always see a consistent state
    //   - Maintaining an invariant that the index always points to a valid entry in the log
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
    w_context: Arc<Mutex<WriteContext>>,
    r_context: ReadContext,
}

impl Clone for KvStore {
    fn clone(&self) -> Self {
        Self {
            w_context: Arc::clone(&self.w_context),
            r_context: self.r_context.clone(),
        }
    }
}

impl KvStore {
    /// Open the key-value store at the given path and return the store to the caller.
    pub fn open<P>(path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let prev_gens = previous_gens(&path)?;
        let gen = prev_gens.last().map(|&e| e + 1).unwrap_or_default();

        // go through all log files, rebuild the index, and keep the handle to each log for later access
        let mut garbage = 0;
        let mut index = BTreeMap::new();
        let mut readers = BTreeMap::new();
        for prev_gen in prev_gens {
            let mut reader = open_log(&path, prev_gen)?;
            garbage += build_index(&mut reader, &mut index, prev_gen)?;
            readers.insert(prev_gen, reader);
        }
        // create a new log file for this instance, taking a write handle and a read handle for it
        let (writer, reader) = create_log(&path, gen)?;
        readers.insert(gen, reader);

        let path = Arc::new(path.as_ref().to_path_buf());
        let index = Arc::new(RwLock::new(index));

        let w_context = WriteContext {
            path: Arc::clone(&path),
            index: Arc::clone(&index),
            writer,
            gen,
            garbage,
        };

        let r_context = ReadContext {
            path: Arc::clone(&path),
            index: Arc::clone(&index),
            readers: RefCell::new(readers),
        };

        Ok(Self {
            w_context: Arc::new(Mutex::new(w_context)),
            r_context,
        })
    }
}

impl KvsEngine for KvStore {
    /// # Error
    ///
    /// Error from I/O operations and serialization/deserialization operations will be propagated.
    fn set(&self, key: String, val: String) -> Result<()> {
        self.w_context.lock().unwrap().set(key, val)
    }

    /// Returns the value of a key, if the key exists. Otherwise, returns `None`.
    ///
    /// # Error
    ///
    /// Error from I/O operations will be propagated.
    fn get(&self, key: String) -> Result<Option<String>> {
        self.r_context.get(key)
    }

    /// Removes a key.
    ///
    /// # Error
    ///
    /// Error from I/O operations will be propagated. If the key doesn't exist returns a
    /// `KeyNotFound` error.
    fn remove(&self, key: String) -> Result<()> {
        todo!("new context types")
    }
}

/// A database's writer that updates on-disk files and maintains consistent index to those files
#[derive(Debug)]
struct WriteContext {
    path: Arc<PathBuf>,
    index: Arc<RwLock<BTreeMap<String, LogIndex>>>,
    writer: BufSeekWriter<File>,
    gen: u64,
    garbage: u64,
}

impl WriteContext {
    fn set(&mut self, key: String, val: String) -> Result<()> {
        let mut index = self.index.write().unwrap();

        let pos = self.writer.pos;
        let log_entry = LogEntry::Set(key.clone(), val);
        bincode::serialize_into(&mut self.writer, &log_entry)?;
        self.writer.flush()?;
        let len = self.writer.pos - pos;

        let log_entry = LogIndex {
            gen: self.gen,
            pos,
            len,
        };
        if let Some(prev_log_entry) = index.insert(key, log_entry) {
            self.garbage += prev_log_entry.len;
            if self.garbage > GARBAGE_THRESHOLD {
                // self.merge()?;
            }
        };

        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        let mut index = self.index.write().unwrap();

        if !index.contains_key(&key) {
            return Err(Error::new(
                ErrorKind::KeyNotFound,
                format!("Key '{}' does not exist", key),
            ));
        }

        let log_entry = LogEntry::Rm(key.clone());
        bincode::serialize_into(&mut self.writer, &log_entry)?;
        self.writer.flush()?;

        if let Some(prev_index) = index.remove(&key) {
            self.garbage += prev_index.len;
            if self.garbage > GARBAGE_THRESHOLD {
                // self.merge()?;
            }
        };
        Ok(())
    }

    fn merge(&mut self) -> Result<()> {
        let merged_gen = self.gen + 1;
        let new_gen = self.gen + 2;

        Ok(())

        //    let (writer, reader) = create_log(self.path.as_ref(), self.epoch)?;
        //    let epoch = self.epoch;
        //    let (mut merged_writer, merged_reader) = create_log(self.path.as_ref(), merged_epoch)?;

        //    let mut new_index_map = self.index_map.clone();
        //    // copy data from old log files to the merged log file and update the in-memory index map
        //    for index in new_index_map.values_mut() {
        //        match self.readers.get_mut(&index.epoch) {
        //            Some(reader) => {
        //                reader.seek(SeekFrom::Start(index.offset))?;
        //                let mut entry_reader = reader.take(index.length);

        //                let merged_offset = merged_writer.stream_position()?;
        //                io::copy(&mut entry_reader, &mut merged_writer)?;

        //                *index = KvsLogEntryIndex {
        //                    epoch: merged_epoch,
        //                    offset: merged_offset,
        //                    length: index.length,
        //                };
        //            }
        //            None => {
        //                return Err(Error::new(
        //                    ErrorKind::CorruptedIndex,
        //                    format!("Could not get reader for epoch #{}", index.epoch),
        //                ))
        //            }
        //        }
        //    }
        //    self.index_map.clear();
        //    self.index_map.clone_from(&new_index_map);
        //    merged_writer.flush()?;

        //    // remove stale log files
        //    let stale_epochs: Vec<u64> = self
        //        .readers
        //        .keys()
        //        .filter(|&&epoch| epoch < merged_epoch)
        //        .cloned()
        //        .collect();
        //    for epoch in stale_epochs {
        //        let log_path = self.path.join(format!("epoch-{}.log", epoch));
        //        fs::remove_file(log_path)?;
        //        self.readers.remove(&epoch);
        //    }
        //    self.garbage = 0;
        //    Ok(())
    }
}

/// A database's reader that reads from on-disk files based on the current index
#[derive(Debug)]
struct ReadContext {
    path: Arc<PathBuf>,
    index: Arc<RwLock<BTreeMap<String, LogIndex>>>,
    readers: RefCell<BTreeMap<u64, BufSeekReader<File>>>,
}

impl Clone for ReadContext {
    fn clone(&self) -> Self {
        // The `ReadContext` will be cloned and sent across threads. Each cloned `ReadContext`
        // will have unique file handles to the log files so that read can happen concurrently
        Self {
            path: Arc::clone(&self.path),
            index: Arc::clone(&self.index),
            readers: RefCell::new(BTreeMap::new()),
        }
    }
}

impl ReadContext {
    fn get(&self, key: String) -> Result<Option<String>> {
        let res = {
            let index = self.index.read().unwrap();
            index.get(&key).cloned()
        };

        match res {
            None => Ok(None),
            Some(index) => {
                let log_entry = {
                    let mut readers = self.readers.borrow_mut();
                    let reader = readers
                        .entry(index.gen)
                        .or_insert(open_log(self.path.as_ref(), index.gen)?);

                    reader.seek(SeekFrom::Start(index.pos))?;
                    bincode::deserialize_from(reader)?
                };

                match log_entry {
                    LogEntry::Set(_, value) => Ok(Some(value)),
                    _ => Err(Error::new(
                        ErrorKind::CorruptedLog,
                        "Expecting a log entry for a set operation",
                    )),
                }
            }
        }
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
    gen: u64,
) -> Result<u64> {
    reader.seek(SeekFrom::Start(0))?;
    let mut garbage = 0;
    loop {
        let pos = reader.pos;
        match bincode::deserialize_from(reader.by_ref()) {
            Ok(e) => match e {
                LogEntry::Set(key, _) => {
                    let len = reader.pos - pos;
                    let index = LogIndex { gen, pos, len };
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
                    // TODO: Note down why this is ok
                    io::ErrorKind::UnexpectedEof => break,
                    _ => return Err(Error::from(err)),
                },
                _ => return Err(Error::from(err)),
            },
        }
    }
    Ok(garbage)
}

fn open_log<P>(path: P, gen: u64) -> Result<BufSeekReader<File>>
where
    P: AsRef<Path>,
{
    let log_path = path.as_ref().join(format!("gen-{}.log", gen));
    let readable_log = OpenOptions::new().read(true).open(&log_path)?;
    let reader = BufSeekReader::new(readable_log)?;
    Ok(reader)
}

fn create_log<P>(path: P, gen: u64) -> Result<(BufSeekWriter<File>, BufSeekReader<File>)>
where
    P: AsRef<Path>,
{
    let log_path = path.as_ref().join(format!("gen-{}.log", gen));

    let writable_log = OpenOptions::new()
        .create_new(true)
        .append(true)
        .open(&log_path)?;
    let readable_log = OpenOptions::new().read(true).open(&log_path)?;

    let writer = BufSeekWriter::new(writable_log)?;
    let reader = BufSeekReader::new(readable_log)?;
    Ok((writer, reader))
}

fn previous_gens<P>(path: P) -> Result<Vec<u64>>
where
    P: AsRef<Path>,
{
    let mut gens: Vec<u64> = fs::read_dir(&path)?
        .filter_map(std::result::Result::ok)
        .map(|e| e.path())
        .filter(|p| p.is_file() && p.extension() == Some("log".as_ref()))
        .filter_map(|p| {
            p.file_stem()
                .and_then(OsStr::to_str)
                .filter(|s| s.starts_with("gen-"))
                .map(|s| s.trim_start_matches("gen-"))
                .map(str::parse::<u64>)
        })
        .filter_map(std::result::Result::ok)
        .collect();
    gens.sort();
    Ok(gens)
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
