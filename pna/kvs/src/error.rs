//! Errors for operations on the key-value store

/// Library result type
pub type Result<T> = std::result::Result<T, Error>;

/// Library error type
pub type Error = Box<ErrorKind>;

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.as_ref() {
            ErrorKind::KeyNotFound => write!(f, "Key not found"),
            ErrorKind::InvalidCommand => write!(f, "Invalid command"),
            ErrorKind::InvalidLogEpoch => write!(f, "Invalid log epoch"),
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
    /// Error occurs when performing operations on non-existent key.
    KeyNotFound,
    /// Error occurs when encounter a command type that is not supposed to be there
    InvalidCommand,
    /// Error occurs when a reader does not exist for some epoch
    InvalidLogEpoch,
    /// Error propagated from I/O operations.
    Io(std::io::Error),
    /// Error propagated from serialization/deserialization operations.
    Bincode(bincode::Error),
}
