//! Provides simple data structures with associated methods that help with storing data in
//! key-value pairs.

#![deny(missing_docs, missing_debug_implementations)]

use std::collections::HashMap;
use std::path::Path;

/// A short-hand for `std::result::Result<T, KvStoreError>`.
pub type Result<T> = std::result::Result<T, Error>;
/// A short-hand for `kvs::KvStoreError`.
pub type Error = KvStoreError;

/// A simple key-value that has supports for inserting, updating, accessing, and removing entries.
/// This implementation holds that key-value inside the main memory that doesn't support data
/// persistence.
///
/// # Example
///
/// ```rust
/// use kvs::KvStore;
///
/// fn main() -> Result<(), Box<dyn std::error::Error>> {
///     // populating the store
///     let mut kvs = KvStore::new();
///     kvs.set("key01".to_string(), "val01".to_string());
///     kvs.set("key02".to_string(), "val02".to_string());
///
///     // accessing entries
///     assert_eq!(Some("val01".to_string()), kvs.get("key01".to_string()));
///     assert_eq!(Some("val02".to_string()), kvs.get("key02".to_string()));
///     assert_eq!(None, kvs.get("key03".to_string()));
///
///     // change entry' value
///     kvs.set("key02".to_string(), "val02-dirty".to_string());
///     assert_eq!(Some("val02-dirty".to_string()), kvs.get("key02".to_string()));
///
///     // remove entry
///     kvs.remove("key02".to_string());
///     kvs.remove("key03".to_string()); // no error if key doesn't exist
///     assert_eq!(None, kvs.get("key02".to_string()));
///
///     Ok(())
/// }
/// ```
#[derive(Debug)]
pub struct KvStore {
    map: HashMap<String, String>,
}

impl Default for KvStore {
    fn default() -> Self {
        Self {
            map: HashMap::new(),
        }
    }
}

impl KvStore {
    /// Open the key-value store that is located at the given path and return the store to the caller.
    pub fn open<P>(_path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        todo!()
    }
    /// Create a new key-value store.
    pub fn new() -> Self {
        Default::default()
    }

    /// Set the value at the given key. If the key already contains a value, the contained value
    /// will be updated to the new value.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.map.insert(key, value);
        Ok(())
    }

    /// Get the value at the given key. If the key doesn't contain a value, the method will return `None`
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        Ok(self.map.get(&key).cloned())
    }

    /// Remove the value at the given key. No error will be reported, if the key doesn't contain a value
    pub fn remove(&mut self, key: String) -> Result<()> {
        self.map.remove(&key);
        Ok(())
    }
}

/// Error type for operations on the key-value store
#[derive(Debug)]
pub enum KvStoreError {}
