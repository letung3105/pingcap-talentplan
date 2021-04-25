//! Errors for operations on the key-value store

/// Library result type
pub type Result<T> = std::result::Result<T, Error>;

/// Library error type
pub type Error = Box<ErrorKind>;

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.as_ref() {
            ErrorKind::InvalidCommand => write!(f, "Invalid command"),
            ErrorKind::KeyNotFound => write!(f, "Key not found"),
            ErrorKind::Io(err) => write!(f, "I/O error {}", err),
            ErrorKind::Bincode(err) => write!(f, "Serialize/Deserialize error {}", err),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Box::new(ErrorKind::Io(err))
    }
}

impl From<bincode::Error> for Error {
    fn from(err: bincode::Error) -> Self {
        Box::new(ErrorKind::Bincode(err))
    }
}

/// All types of error that can occur
#[derive(Debug)]
pub enum ErrorKind {
    /// Invalid command
    InvalidCommand,
    /// Error when performing operations on non-existent key.
    KeyNotFound,
    /// Error from I/O operations.
    Io(std::io::Error),
    /// Error from serialization/deserialization operations.
    Bincode(bincode::Error),
}
