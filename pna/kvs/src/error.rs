//! Errors for operations on the key-value store

use std::error;

/// Library result
pub type Result<T> = std::result::Result<T, Error>;

/// Library error
#[derive(Debug)]
pub struct Error {
    repr: Repr,
}

#[derive(Debug)]
enum Repr {
    /// Specific error with no additional messages
    Simple(ErrorKind),
    Custom(Box<CustomRepr>),

    /// I/O error
    Io(std::io::Error),
    /// Sled error
    Sled(sled::Error),
    /// Bincode error
    Bincode(bincode::Error),
    /// Serde JSON error
    SerdeJson(serde_json::Error),
    /// Rayon ThreadPoolBuildError
    RayonThreadPoolBuildError(rayon::ThreadPoolBuildError),
}

impl Error {
    /// Create a new error
    pub fn new<E>(kind: ErrorKind, error: E) -> Error
    where
        E: Into<Box<dyn error::Error + Send + Sync>>,
    {
        let error = error.into();
        Self {
            repr: Repr::Custom(Box::new(CustomRepr { kind, error })),
        }
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Self {
            repr: Repr::Simple(kind),
        }
    }
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.repr {
            Repr::Simple(ref kind) => write!(f, "{}", kind.as_str()),
            Repr::Custom(ref repr) => write!(f, "{} ({})", repr.error, repr.kind.as_str()),
            Repr::Io(ref err) => write!(f, "{} (i/o error)", err),
            Repr::Sled(ref err) => write!(f, "{} (sled error)", err),
            Repr::Bincode(ref err) => write!(f, "{} (bincode (de)serialization error)", err),
            Repr::SerdeJson(ref err) => write!(f, "{} (json (de)serialization error)", err),
            Repr::RayonThreadPoolBuildError(ref err) => {
                write!(f, "{} (rayon's thread pool build error)", err)
            }
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self {
            repr: Repr::Io(err),
        }
    }
}

impl From<sled::Error> for Error {
    fn from(err: sled::Error) -> Self {
        Self {
            repr: Repr::Sled(err),
        }
    }
}

impl From<bincode::Error> for Error {
    fn from(err: bincode::Error) -> Self {
        Self {
            repr: Repr::Bincode(err),
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Self {
            repr: Repr::SerdeJson(err),
        }
    }
}

impl From<rayon::ThreadPoolBuildError> for Error {
    fn from(err: rayon::ThreadPoolBuildError) -> Self {
        Self {
            repr: Repr::RayonThreadPoolBuildError(err),
        }
    }
}

#[derive(Debug)]
struct CustomRepr {
    kind: ErrorKind,
    error: Box<dyn error::Error + Send + Sync>,
}

/// Types of error
#[derive(Debug)]
#[non_exhaustive]
pub enum ErrorKind {
    /// Operation on a non-existent key
    KeyNotFound,
    /// Faulty on-disk log
    CorruptedLog,
    /// Faulty in-memory index
    CorruptedIndex,
    /// An unexpected message from the network is received
    InvalidNetworkMessage,
    /// Wrong engine provided when constructing a key-value store
    UnsupportedKvsEngine,
    /// Error that was originated from the remote server
    ServerError,
}

impl ErrorKind {
    pub(crate) fn as_str(&self) -> &'static str {
        match *self {
            Self::KeyNotFound => "Key not found",
            Self::CorruptedLog => "Corrupted on-disk log",
            Self::CorruptedIndex => "Corrupted in-memory index",
            Self::InvalidNetworkMessage => "Received an invalid network message",
            Self::UnsupportedKvsEngine => "Unsupported key-value store engine",
            Self::ServerError => "Remote server error",
        }
    }
}
