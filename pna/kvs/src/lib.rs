//! Provides simple data structures with associated methods that help with storing data in
//! key-value pairs.

#![deny(missing_docs, missing_debug_implementations)]

#[macro_use]
extern crate slog;

pub mod engines;
pub mod error;
pub mod proto;

pub use engines::{KvStore, KvsEngine, SledKvsEngine};
pub use error::{Error, ErrorKind, Result};
pub use proto::{KvsClient, KvsServer};
