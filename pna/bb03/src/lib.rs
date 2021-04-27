//! Simple client/service implementation of RESP (REdis Serialization Protocol). Only a subsets of the Redis available
//! commands is implemented.

#![deny(missing_docs, missing_debug_implementations)]

use std::error;
use std::fmt;
use std::io;
use std::net;

/// IP address for testing the client/server on the local machine
pub const TEST_ADDR: ([u8; 4], u16) = ([127, 0, 0, 1], 8080);

/// Result for operations on the RESP client
pub type Result<T> = std::result::Result<T, Error>;

/// Custom RESP client
#[derive(Debug)]
pub struct BluisClient {
    addr_remote: net::SocketAddr,
}

impl BluisClient {
    /// Create a new client the communicates in  Eprotocoler otocol
    pub fn new<A>(addr_remote: A) -> Self
    where
        A: Into<net::SocketAddr>,
    {
        let addr_remote = addr_remote.into();
        Self { addr_remote }
    }

    /// Send a `PING` command to the RESP server
    pub fn ping(&self, _message: String) -> Result<()> {
        todo!()
    }
}

/// Error from operations on the custom RESP client and server
#[derive(Debug)]
pub struct Error(Box<ErrorKind>);

impl error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0.as_ref() {
            ErrorKind::IoError(e) => write!(f, "I/O error occured {}", e),
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self(Box::new(ErrorKind::IoError(e)))
    }
}

/// Types of error that can occur with the custom RESP client and server
#[derive(Debug)]
pub enum ErrorKind {
    /// Propagated error from I/O operations
    IoError(io::Error),
}
