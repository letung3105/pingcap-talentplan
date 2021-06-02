//! An `KvsEngine` that proxies method calls to the underlying `sled` key-value store.

use crate::{Error, ErrorKind, KvsEngine, Result};
use std::path::PathBuf;

/// A key-value store that uses sled as the underlying data storage engine
#[derive(Debug, Clone)]
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
        let db = sled::Config::default().path(active_path).open()?;
        Ok(Self { db })
    }
}

impl KvsEngine for SledKvsEngine {
    fn set(&self, key: String, value: String) -> Result<()> {
        self.db.insert(key, value.as_bytes())?;
        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        self.db
            .get(key.as_bytes())
            .map(|val| {
                // NOTE: Since the value is inserted as a string, using unwrap is ok
                val.map(|iv| iv.to_vec())
                    .map(|v| String::from_utf8(v).unwrap())
            })
            .map_err(Error::from)
    }

    fn remove(&self, key: String) -> Result<()> {
        self.db.remove(key.as_bytes())?.ok_or(Error::new(
            ErrorKind::KeyNotFound,
            format!("Key '{}' does not exist", key),
        ))?;
        Ok(())
    }
}
