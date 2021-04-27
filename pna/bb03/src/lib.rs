#![deny(missing_debug_implementations)]

use std::fmt;
use std::io::{self, BufReader, BufWriter};
use std::net::{SocketAddr, TcpStream};

pub const TEST_ADDR: ([u8; 4], u16) = ([127, 0, 0, 1], 8080);

#[derive(Debug)]
pub struct BluisClient {
    stream_reader: BufReader<TcpStream>,
    stream_writer: BufWriter<TcpStream>,
}

impl BluisClient {
    pub fn new<A>(server_addr: A) -> Result<Self>
    where
        A: Into<SocketAddr>,
    {
        let stream = TcpStream::connect(server_addr.into())?;
        let rstream = stream.try_clone()?;
        let stream_reader = BufReader::new(rstream);
        let stream_writer = BufWriter::new(stream);

        Ok(Self {
            stream_reader,
            stream_writer,
        })
    }

    pub fn ping(&self, _message: String) -> Result<()> {
        Ok(())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub struct Error(Box<ErrorKind>);

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> fmt::Result {
        match self.0.as_ref() {
            ErrorKind::IoError(e) => write!(f, "I/O error {}", e),
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self(Box::new(ErrorKind::IoError(e)))
    }
}

#[derive(Debug)]
pub enum ErrorKind {
    IoError(io::Error),
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
