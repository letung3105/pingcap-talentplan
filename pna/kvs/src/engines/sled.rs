//! An `KvsEngine` that proxies method calls to the underlying `sled` key-value store.

use crate::engines::{KvsEngineBackend, KVS_ENGINE_BACKEND_FILENAME};
use crate::{Error, ErrorKind, KvsEngine, Result};
use std::fs;
use std::path::PathBuf;

/// A key-value store that uses sled as the underlying data storage engine
#[derive(Debug)]
pub struct SledKvsEngine {
    db: sled::Db,
}

impl SledKvsEngine {
    /// Start the storage engine with the file system created at the given path
    pub fn open<P>(path: P) -> Result<Self>
    where
        P: Into<PathBuf>,
    {
        let active_path = path.into();
        let backend_path = active_path.join(KVS_ENGINE_BACKEND_FILENAME);
        fs::write(backend_path, KvsEngineBackend::Sled.as_str())?;

        let db = sled::Config::default().path(active_path).open()?;
        Ok(Self { db })
    }
}

// NOTE: We are flushing the in-memory data on every write/remove operation since the current test
// forcefully kill the server before it has a chance to cleanup. This causes the data store to run
// very slow and it should be changed once the test from pingcap is updated
impl KvsEngine for SledKvsEngine {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.db.insert(key, value.as_bytes())?;
        self.db.flush()?;
        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        self.db
            .get(key.as_bytes())
            .map(|val| {
                // NOTE: Since the value is inserted as a string, using unwrap is ok
                val.map(|iv| iv.to_vec())
                    .map(|v| String::from_utf8(v).unwrap())
            })
            .map_err(Error::from)
    }

    fn remove(&mut self, key: String) -> Result<()> {
        self.db
            .remove(key.as_bytes())?
            .ok_or(Error::from(ErrorKind::KeyNotFound))?;
        self.db.flush()?;
        Ok(())
    }
}
