//! Simple client/service implementation of RESP (REdis Serialization Protocol). Only a subsets of the Redis available
//! commands is implemented.

#![deny(missing_docs, missing_debug_implementations)]

use std::error;
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
            stream_reader,
            stream_writer,
        })
    }

    /// Send a `PING` command to the RESP server
    pub fn ping(&mut self, message: String) -> Result<String> {
        static PING_COMMAND: &[u8] = b"PING";
        let mut packet = Vec::new();
        // array size
        packet.extend_from_slice(b"*2");
        packet.extend_from_slice(&CRLF);
        // command
        packet.extend_from_slice(b"$");
        packet.extend_from_slice(PING_COMMAND.len().to_string().as_bytes());
        packet.extend_from_slice(&CRLF);
        packet.extend_from_slice(PING_COMMAND);
        packet.extend_from_slice(&CRLF);
        // argument(s)
        packet.extend_from_slice(b"$");
        packet.extend_from_slice(message.len().to_string().as_bytes());
        packet.extend_from_slice(&CRLF);
        packet.extend_from_slice(message.as_bytes());
        packet.extend_from_slice(&CRLF);
        // send command
        self.stream_writer.write_all(&packet)?;

        let mut reply_size_buf = Vec::new();
        self.stream_reader.consume(1);
        self.stream_reader.read_until(b'\r', &mut reply_size_buf)?;
        self.stream_reader.consume(2);

        let reply_size = String::from_utf8_lossy(&reply_size_buf).parse().unwrap();
        let mut reply_buf = Vec::with_capacity(reply_size);
        self.stream_reader.read_exact(&mut reply_buf)?;

        println!(
            "Encoded ping command:\n{}",
            String::from_utf8_lossy(&packet)
        );

        println!("Reply:\n{}", String::from_utf8_lossy(&reply_buf));

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
