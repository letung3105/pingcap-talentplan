//! Module for handling network communication between client and server

mod json;

pub use json::*;

use crate::Result;
use std::net::SocketAddr;

/// Client interface
pub trait KvsClient {
    /// Connect to the remote server
    fn connect<A>(addr: A) -> Result<Self>
    where
        Self: Sized,
        A: Into<SocketAddr>;
    /// Send set command
    fn set(&mut self, key: String, value: String) -> Result<()>;
    /// Send get command
    fn get(&mut self, key: String) -> Result<Option<String>>;
    /// Send remove command
    fn remove(&mut self, key: String) -> Result<()>;
}

/// Server interface
pub trait KvsServer {
    /// Start accepting requests on the given socket address
    fn serve<A>(&mut self, addr: A) -> Result<()>
    where
        A: Into<SocketAddr>;
}
