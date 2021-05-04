//! Providing network API for interacting with the key-value store implementation

mod proto {
    include!(concat!(env!("OUT_DIR"), "/kvs.network.proto.rs"));
}

use bytes::{BufMut, BytesMut};
use prost::Message;
use proto::kvs_request::KvsRequestKind;
use proto::kvs_response::ResponseResult;
use proto::{KvsRequest, KvsResponse};
use std::io::{BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;

use crate::{Error, ErrorKind, KvStore, KvsEngine, KvsEngineVariant, Result};

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
    pub fn set_req(&self, key: String, value: String) -> Result<()> {
        let req = KvsRequest {
            kind: KvsRequestKind::Set as i32,
            key,
            value,
        };

        let res = self.make_request(req)?;
        match res.response_result {
            Some(result) => match result {
                ResponseResult::ErrorMessage(msg) => Err(Error::new(ErrorKind::ServerError(msg))),
                _ => Err(Error::new(ErrorKind::InvalidKvsResponse)),
            },
            None => Ok(()),
        }
    }

    /// Send get command request to the key-val store's server
    pub fn get_req(&self, key: String) -> Result<Option<String>> {
        let req = KvsRequest {
            kind: KvsRequestKind::Get as i32,
            key,
            value: String::default(),
        };

        let res = self.make_request(req)?;
        match res.response_result {
            Some(result) => match result {
                ResponseResult::ErrorMessage(msg) => Err(Error::new(ErrorKind::ServerError(msg))),
                ResponseResult::GetCommandValue(value) => Ok(Some(value)),
            },
            None => Ok(None),
        }
    }

    /// Send remove command request to the key-val store's server
    pub fn remove_req(&self, key: String) -> Result<()> {
        let req = KvsRequest {
            kind: KvsRequestKind::Remove as i32,
            key,
            value: String::default(),
        };

        let res = self.make_request(req)?;
        match res.response_result {
            Some(result) => match result {
                ResponseResult::ErrorMessage(msg) => Err(Error::new(ErrorKind::ServerError(msg))),
                _ => Err(Error::new(ErrorKind::InvalidKvsResponse)),
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

        // NOTE: before the length delimiter can be parsed, we will reading from stream one byte at a time,
        // until the bytes that represent the length delimiter is fully received. This is done mainly to avoid
        // consuming the bytes that belong to the next message from the TcpStream when the currently processed
        // message is very small
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

/// Implementation of a server that listens for client requests, and performs the received commands
/// on the underlying key-value storage engine
#[derive(Debug)]
pub struct KvsServer {
    kvs_engine: Box<dyn KvsEngine>,
}

impl KvsServer {
    /// Create a new key-value store client.
    pub fn new<P>(engine_variant: KvsEngineVariant, data_path: P) -> Result<Self>
    where
        P: Into<PathBuf>,
    {
        let data_path = data_path.into();
        let kvs_engine = Box::new(match engine_variant {
            KvsEngineVariant::Kvs => KvStore::open(data_path)?,
            KvsEngineVariant::Sled => todo!(),
        });

        Ok(Self { kvs_engine })
    }

    /// Starting accepting requests on the given IP address and modify the key-value store
    /// based on the received command
    pub fn serve<A>(&mut self, addr: A) -> Result<()>
    where
        A: Into<SocketAddr>,
    {
        let listener = TcpListener::bind(addr.into())?;
        for stream in listener.incoming() {
            if let Ok(mut stream) = stream {
                if let Err(err) = self.handle_client(stream.try_clone()?) {
                    eprintln!("Could not handle client {}", err);
                    let res = KvsResponse {
                        response_result: Some(ResponseResult::ErrorMessage(err.to_string())),
                    };
                    let mut res_buf = vec![];
                    res.encode_length_delimited(&mut res_buf)?;
                    stream.write_all(&res_buf)?;
                }
            }
        }
        Ok(())
    }

    fn handle_client(&mut self, stream: TcpStream) -> Result<()> {
        let mut stream_reader = BufReader::new(stream.try_clone()?);
        let mut len_delim_buf = [0u8; 10];
        let mut msg_len_delim = BytesMut::new();

        loop {
            let n_read = stream_reader.read(&mut len_delim_buf)?;
            msg_len_delim.put_slice(&len_delim_buf[..n_read]);

            match prost::decode_length_delimiter(msg_len_delim.as_ref()) {
                Ok(len) => {
                    let len_delim_length = prost::length_delimiter_len(len);
                    let n_remaining = len - (msg_len_delim.len() - len_delim_length);

                    let mut msg_remaining = vec![0u8; n_remaining];
                    stream_reader.read_exact(&mut msg_remaining)?;
                    msg_len_delim.put_slice(&msg_remaining);

                    let req = KvsRequest::decode(msg_len_delim.split_off(len_delim_length))?;
                    match KvsRequestKind::from_i32(req.kind) {
                        Some(KvsRequestKind::Set) => {
                            return self.handle_set(stream, req.key, req.value)
                        }
                        Some(KvsRequestKind::Get) => return self.handle_get(stream, req.key),
                        Some(KvsRequestKind::Remove) => return self.handle_remove(stream, req.key),
                        None => return Err(Error::new(ErrorKind::InvalidKvsRequest)),
                    }
                }
                Err(err) => {
                    if msg_len_delim.len() > 10 {
                        return Err(Error::from(err));
                    }
                }
            };
        }
    }

    fn handle_set(&mut self, mut stream: TcpStream, key: String, value: String) -> Result<()> {
        self.kvs_engine.set(key, value)?;
        let res = KvsResponse {
            response_result: None,
        };
        let mut res_buf = vec![];
        res.encode_length_delimited(&mut res_buf)?;
        stream.write_all(&res_buf)?;
        Ok(())
    }

    fn handle_get(&mut self, mut stream: TcpStream, key: String) -> Result<()> {
        let value = self.kvs_engine.get(key)?;
        let res = KvsResponse {
            response_result: value.map(|val| ResponseResult::GetCommandValue(val)),
        };
        let mut res_buf = vec![];
        res.encode_length_delimited(&mut res_buf)?;
        stream.write_all(&res_buf)?;
        Ok(())
    }

    fn handle_remove(&mut self, mut stream: TcpStream, key: String) -> Result<()> {
        self.kvs_engine.remove(key)?;
        let res = KvsResponse {
            response_result: None,
        };
        let mut res_buf = vec![];
        res.encode_length_delimited(&mut res_buf)?;
        stream.write_all(&res_buf)?;
        Ok(())
    }
}
