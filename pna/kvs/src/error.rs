//! Errors for operations on the key-value store

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
    /// Error message returned from the remote server
    RemoteMessage(String),

    /// I/O error
    Io(std::io::Error),
    /// Sled error
    Sled(sled::Error),
    /// Bincode error
    Bincode(bincode::Error),
    /// Prost serialization error
    ProstEncode(prost::EncodeError),
    /// Prost deserialization error
    ProstDecode(prost::DecodeError),
}

impl Error {
    /// Create an error from the error message that was received from a remote service
    pub fn from_remote(msg: String) -> Self {
        Self {
            repr: Repr::RemoteMessage(msg),
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
            Repr::RemoteMessage(ref msg) => write!(f, "{} (error from remote)", msg),
            Repr::Io(ref err) => write!(f, "{} (i/o error)", err),
            Repr::Sled(ref err) => write!(f, "{} (sled error)", err),
            Repr::Bincode(ref err) => write!(f, "{} (bincode (de)serialization error)", err),
            Repr::ProstEncode(ref err) => write!(f, "{} (protobuf serialization error)", err),
            Repr::ProstDecode(ref err) => write!(f, "{} (protobuf deserialization error(", err),
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

impl From<prost::EncodeError> for Error {
    fn from(err: prost::EncodeError) -> Self {
        Self {
            repr: Repr::ProstEncode(err),
        }
    }
}

impl From<prost::DecodeError> for Error {
    fn from(err: prost::DecodeError) -> Self {
        Self {
            repr: Repr::ProstDecode(err),
        }
    }
}

/// Types of error
#[derive(Debug)]
pub enum ErrorKind {
    /// Operation on a non-existent key
    KeyNotFound,
    /// Faulty on-disk log
    CorruptedLog,
    /// Faulty in-memory index
    CorruptedIndex,
    /// An unexpected message from the network is received
    InvalidNetworkMessage,
    /// Wrong engine's name provided when constructing a key-value store
    UnsupportedKvsEngine,
    /// Provided engine's name is different from the one used in the data directory
    MismatchedKvsEngine,
}

impl ErrorKind {
    pub(crate) fn as_str(&self) -> &'static str {
        match *self {
            Self::KeyNotFound => "Key not found",
            Self::CorruptedLog => "Corrupted on-disk log",
            Self::CorruptedIndex => "Corrupted in-memory index",
            Self::InvalidNetworkMessage => "Received an invalid network message",
            Self::UnsupportedKvsEngine => "Unsupported key-value store engine",
            Self::MismatchedKvsEngine => "Mismatched key-value store engine",
        }
    }
}
