use crate::proto::messages::kvs_request::KvsRequestKind;
use crate::proto::messages::kvs_response::ResponseResult;
use crate::proto::messages::{KvsRequest, KvsResponse};
use crate::{Error, ErrorKind, Result};
use bytes::{BufMut, BytesMut};
use prost::Message;
use std::io::{BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};

/// Implementation of a client that can communicate with the system's server
#[derive(Debug)]
pub struct KvsClient {
    server_addr: SocketAddr,
}

impl KvsClient {
    /// Create a new key-value store client.
    pub fn new<A>(addr: A) -> Self
    where
        A: Into<SocketAddr>,
    {
        let server_addr = addr.into();
        Self { server_addr }
    }

    /// Send set command request to the key-val store's server
    pub fn set(&self, key: String, value: String) -> Result<()> {
        let req = KvsRequest {
            kind: KvsRequestKind::Set as i32,
            key,
            value,
        };

        let res = self.make_request(req)?;
        match res.response_result {
            Some(result) => match result {
                ResponseResult::ErrorMessage(msg) => Err(Error::new(ErrorKind::ServerError, msg)),
                _ => Err(Error::new(
                    ErrorKind::InvalidNetworkMessage,
                    "Expecting an empty response",
                )),
            },
            None => Ok(()),
        }
    }

    /// Send get command request to the key-val store's server
    pub fn get(&self, key: String) -> Result<Option<String>> {
        let req = KvsRequest {
            kind: KvsRequestKind::Get as i32,
            key,
            value: String::default(),
        };

        let res = self.make_request(req)?;
        match res.response_result {
            Some(result) => match result {
                ResponseResult::ErrorMessage(msg) => Err(Error::new(ErrorKind::ServerError, msg)),
                ResponseResult::GetCommandValue(value) => Ok(Some(value)),
            },
            None => Ok(None),
        }
    }

    /// Send remove command request to the key-val store's server
    pub fn remove(&self, key: String) -> Result<()> {
        let req = KvsRequest {
            kind: KvsRequestKind::Remove as i32,
            key,
            value: String::default(),
        };

        let res = self.make_request(req)?;
        match res.response_result {
            Some(result) => match result {
                ResponseResult::ErrorMessage(msg) => Err(Error::new(ErrorKind::ServerError, msg)),
                _ => Err(Error::new(
                    ErrorKind::InvalidNetworkMessage,
                    "Expecting an empty response",
                )),
            },
            None => Ok(()),
        }
    }

    fn make_request(&self, req: KvsRequest) -> Result<KvsResponse> {
        let mut request_bytes = vec![];
        req.encode_length_delimited(&mut request_bytes)?;
        let mut stream = TcpStream::connect(self.server_addr)?;
        stream.write_all(&request_bytes)?;

        let mut len_delim_bytes = [0u8; 1];
        let mut msg_bytes = BytesMut::new();
        let mut stream_reader = BufReader::new(stream);

        // NOTE: Before the length delimiter can be parsed, we will reading from stream one byte at
        // a time, until the bytes that represent the length delimiter is fully received. This is
        // done mainly to avoid consuming the bytes that belong to the next message from the
        // TcpStream when the currently processed message is very small. The cost of doing this is
        // very high, but as we progress, a better protocol will be devised.
        loop {
            let n_read = stream_reader.read(&mut len_delim_bytes)?;
            msg_bytes.put_slice(&len_delim_bytes[..n_read]);

            match prost::decode_length_delimiter(msg_bytes.as_ref()) {
                Ok(len) => {
                    let len_delim_bytes_len = prost::length_delimiter_len(len);
                    let n_remaining = len - (msg_bytes.len() - len_delim_bytes_len);

                    let mut msg_bytes_remaining = vec![0u8; n_remaining];
                    stream_reader.read_exact(&mut msg_bytes_remaining)?;
                    msg_bytes.put_slice(&msg_bytes_remaining);

                    return Ok(KvsResponse::decode(
                        msg_bytes.split_off(len_delim_bytes_len),
                    )?);
                }
                Err(err) => {
                    if msg_bytes.len() > 10 {
                        return Err(Error::from(err));
                    }
                }
            }
        }
    }
}
