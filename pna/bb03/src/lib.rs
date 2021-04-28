//! Simple client/service implementation of RESP (REdis Serialization Protocol). Only a subsets of the Redis available
//! commands is implemented.

#![deny(missing_docs, missing_debug_implementations)]

use std::fmt;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{self, TcpStream};
use std::{error, net::SocketAddr};

/// Encoded type termination
pub const CRLF: [u8; 2] = [b'\r', b'\n'];

/// IP address for testing the client/server on the local machine
pub const TEST_ADDR: ([u8; 4], u16) = ([127, 0, 0, 1], 8080);

/// Result for operations on the RESP client
pub type Result<T> = std::result::Result<T, Error>;

/// Custom RESP client
#[derive(Debug)]
pub struct BluisClient {
    addr_remote: SocketAddr,
    stream: TcpStream,
    stream_reader: BufReader<TcpStream>,
}

impl BluisClient {
    /// Create a new client the communicates in  Eprotocoler otocol
    pub fn new<A>(addr_remote: A) -> Result<Self>
    where
        A: Into<net::SocketAddr>,
    {
        let addr_remote = addr_remote.into();
        let stream = TcpStream::connect(addr_remote)?;
        let stream_reader = BufReader::new(stream.try_clone()?);

        Ok(Self {
            addr_remote,
            stream,
            stream_reader,
        })
    }

    /// Send a `PING` command to the RESP server
    pub fn ping(&mut self, message: Option<String>) -> Result<String> {
        let mut packet = Vec::new();
        match message {
            // encode a PING command with no argument
            None => packet.extend_from_slice(b"*1\r\n$4\r\nPING\r\n"),
            // encode a PING command with an argument
            Some(m) => {
                packet.extend_from_slice(b"*2\r\n$4\r\nPING\r\n");
                packet.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                packet.extend_from_slice(format!("{}\r\n", m).as_bytes());
            }
        }
        println!("Encoded ping command: {:?}", packet);
        self.stream.write_all(&packet)?;

        // get bulk string's length
        let mut resp_len_buf = vec![];
        self.stream_reader.read_exact(&mut [0; 1])?;
        self.stream_reader.read_until(b'\r', &mut resp_len_buf)?;
        self.stream_reader.read_exact(&mut [0; 1])?;

        let resp_len_buf = match resp_len_buf.split_last() {
            None => Vec::new(),
            Some((_, until_last)) => Vec::from(until_last),
        };
        let resp_len = String::from_utf8(resp_len_buf).unwrap().parse().unwrap();
        println!("Response length: {:?}", resp_len);

        // get bulk string's content
        let mut resp_buf = vec![0u8; resp_len];
        self.stream_reader.read_exact(&mut resp_buf)?;
        self.stream_reader.read_exact(&mut [0; 2])?;
        println!("Response bytes: {:?}", resp_buf);

        let resp_string = String::from_utf8(resp_buf).unwrap();
        println!("Response text: {:?}", resp_string);
        Ok(resp_string)
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
