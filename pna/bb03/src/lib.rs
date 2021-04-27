//! Simple client/service implementation of RESP (REdis Serialization Protocol). Only a subsets of the Redis available
//! commands is implemented.

#![deny(missing_docs, missing_debug_implementations)]

use std::{error, net::SocketAddr};
use std::fmt;
use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use std::net::{self, TcpStream};

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
    stream_reader: BufReader<TcpStream>,
    stream_writer: BufWriter<TcpStream>,
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
        let stream_writer = BufWriter::new(stream.try_clone()?);
        Ok(Self {
            addr_remote,
            stream_reader,
            stream_writer,
        })
    }

    /// Send a `PING` command to the RESP server
    pub fn ping(&mut self, message: String) -> Result<String> {
        let mut stream = TcpStream::connect(self.addr_remote)?;

        let mut packet = Vec::new();
        packet.extend_from_slice(b"*2\r\n$4\r\nPING\r\n");
        packet.extend_from_slice(format!("${}\r\n", message.len()).as_bytes());
        packet.extend_from_slice(format!("{}\r\n", message).as_bytes());

        println!("Encoded ping command: {:?}", packet);
        stream.write_all(&packet)?;


        let mut reply_size_buf = vec![];
        self.stream_reader.consume(1);
        self.stream_reader.read_until(b'\r', &mut reply_size_buf)?;
        self.stream_reader.consume(2);
        let reply_size = String::from_utf8(reply_size_buf).unwrap().parse().unwrap();
        println!("Reply size: {:?}", reply_size);

        let mut reply_buf = vec![0u8; reply_size];
        self.stream_reader.read_exact(&mut reply_buf)?;
        println!("Reply (bytes): {:?}", reply_buf);

        let reply_string = String::from_utf8(reply_buf).unwrap();
        println!("Reply (string): {:?}", reply_string);

        Ok(reply_string)
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
