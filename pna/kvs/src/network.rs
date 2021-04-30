//! Providing network API for interacting with the key-value store implementation

use crate::{KvsEngineVariant, Result};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::PathBuf;

/// Implementation of a client that can communicate with the system's server
#[derive(Debug)]
pub struct KvsClient {
    server_addr: SocketAddr,
}

impl KvsClient {
    /// Create a new key-value store client.
    pub fn new<A>(addr: A) -> Self
    where
        A: Into<SocketAddr>,
    {
        let server_addr = addr.into();
        Self { server_addr }
    }

    /// Send set command request to the key-val store's server
    pub fn set_req(&self, key: String, value: String) -> Result<()> {
        todo!()
    }

    /// Send get command request to the key-val store's server
    pub fn get_req(&self, key: String) -> Result<Option<String>> {
        todo!()
    }

    /// Send remove command request to the key-val store's server
    pub fn remove_req(&self, key: String) -> Result<()> {
        todo!()
    }
}

/// Implementation of a server that listens for client requests, and performs the received commands
/// on the underlying key-value storage engine
#[derive(Debug)]
pub struct KvsServer {
    engine_variant: KvsEngineVariant,
    data_path: PathBuf,
}

impl KvsServer {
    /// Create a new key-value store client.
    pub fn new<P>(engine_variant: KvsEngineVariant, data_path: P) -> Self
    where
        P: Into<PathBuf>,
    {
        let data_path = data_path.into();
        Self {
            engine_variant,
            data_path,
        }
    }

    /// Starting accepting requests on the given IP address and modify the key-value store
    /// based on the received command
    pub fn serve<A>(addr: A)
    where
        A: Into<SocketAddr>,
    {
        todo!()
    }
}

/// Data structure for holding the content of messages exchanged between the client and the server
#[derive(Debug, Serialize, Deserialize)]
enum KvsMessage {
    Set(String, String),
    Get(String),
    Remove(String),
}
