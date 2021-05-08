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

// TODO: might need to call flush on every write
impl KvsEngine for SledKvsEngine {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.db.insert(key.as_bytes(), value.as_bytes())?;
        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        let value = self.db.get(key.as_bytes())?;
        // NOTE: since the value is inserted as a string, using unwrap here is ok.
        let value = value.map(|v| String::from_utf8(v.to_vec()).unwrap());
        Ok(value)
    }

    fn remove(&mut self, key: String) -> Result<()> {
        match self.db.remove(key.as_bytes())? {
            Some(_) => Ok(()),
            None => Err(Error::new(ErrorKind::KeyNotFound)),
        }
    }
}
