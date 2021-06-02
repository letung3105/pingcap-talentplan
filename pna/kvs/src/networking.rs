//! Module for handling network communication between client and server

use std::{
    io::{BufReader, BufWriter},
    net::{SocketAddr, TcpStream},
};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Request {
    Set { key: String, value: String },
    Get { key: String },
    Remove { key: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum SetResponse {
    Ok(()),
    Err(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum GetResponse {
    Ok(Option<String>),
    Err(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum RemoveResponse {
    Ok(()),
    Err(String),
}

/// Network client for JSON message
#[derive(Debug)]
pub struct KvsClient {
    rstream: BufReader<TcpStream>,
    wstream: BufWriter<TcpStream>,
}

impl KvsClient {
    /// Connect to the remote server at `addr` and return the client to it
    pub fn connect<A>(addr: A) -> Self
    where
        A: Into<SocketAddr>,
    {
        todo!()
    }
}
