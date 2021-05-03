//! Errors for operations on the key-value store

/// Library result type
pub type Result<T> = std::result::Result<T, Error>;

/// Library error type
pub type Error = Box<ErrorKind>;

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.as_ref() {
            ErrorKind::InvalidLogEntry => write!(f, "Invalid command"),
            ErrorKind::InvalidLogIndex => write!(f, "Invalid log epoch"),
            ErrorKind::InvalidKvsEngineVariant => {
                write!(f, "Could not parse key-value store engine variant")
            }
            ErrorKind::InvalidKvsRequest => write!(f, "Invalid request from the client"),
            ErrorKind::InvalidKvsResponse => write!(f, "Invalid response from the server"),
            ErrorKind::KeyNotFound => write!(f, "Key not found"),
            ErrorKind::ServerError(msg) => write!(f, "Server-side error occurred {}", msg),
            ErrorKind::Io(err) => write!(f, "I/O error {}", err),
            ErrorKind::Bincode(err) => write!(f, "Bincode serde error {}", err),
            ErrorKind::ProstEncode(err) => write!(f, "Protobuf serialization error {}", err),
            ErrorKind::ProstDecode(err) => write!(f, "Protobuf deserialization error {}", err),
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

impl From<prost::EncodeError> for Error {
    fn from(err: prost::EncodeError) -> Self {
        Box::new(ErrorKind::ProstEncode(err))
    }
}

impl From<prost::DecodeError> for Error {
    fn from(err: prost::DecodeError) -> Self {
        Box::new(ErrorKind::ProstDecode(err))
    }
}

/// All types of error that can occur
#[derive(Debug)]
pub enum ErrorKind {
    /// Error occurs when encounter a command type that is not supposed to be there
    InvalidLogEntry,
    /// Error occurs when the index points the a non-existent item
    InvalidLogIndex,
    /// Error occurs when trying to parse a `KvsEngineVariant`
    InvalidKvsEngineVariant,
    /// Error for when receiving an unexpected request from the client
    InvalidKvsRequest,
    /// Error for when receiving an unexpected reponse from the server
    InvalidKvsResponse,
    /// Error occurs when performing operations on non-existent key
    KeyNotFound,
    /// Error occurs on the server that is sent back to the client
    ServerError(String),
    /// Error propagated from I/O operations
    Io(std::io::Error),
    /// Error propagated from serialization/deserialization operations with bincode
    Bincode(bincode::Error),
    /// Error propagated from serialization operations with protocol buffers
    ProstEncode(prost::EncodeError),
    /// Error propagated from deserialization operations with protocol buffers
    ProstDecode(prost::DecodeError),
}
