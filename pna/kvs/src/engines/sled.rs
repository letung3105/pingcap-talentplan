//! An `KvsEngine` that proxies method calls to the underlying `sled` key-value store.

use std::path::PathBuf;

use crate::{Error, ErrorKind, KvsEngine, Result};

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
        let db = sled::open(path.into())?;
        Ok(Self { db })
    }
}

impl KvsEngine for SledKvsEngine {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.db.insert(key, value.as_bytes())?;
        self.db.flush()?;
        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        // NOTE: since the value is inserted as a string, using unwrap is ok
        self.db
            .get(key.as_bytes())
            .map(|val| {
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
