//! An `KvsEngine` that proxies method calls to the underlying `sled` key-value store.

use crate::{Error, ErrorKind, KvsEngine, Result};

/// A key-value store that uses sled as the underlying data storage engine
#[derive(Debug, Clone)]
pub struct SledKvsEngine {
    db: sled::Db,
}

impl SledKvsEngine {
    /// Creates a new proxy that forwards method calls to the underlying key-value store
    pub fn new(db: sled::Db) -> Self {
        Self { db }
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
