//! Providing network API for interacting with the key-value store implementation

use bytes::{BufMut, BytesMut};
use prost::Message;
use std::io::{BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;

use crate::engines::{KvStore,SledKvsEngine};
use crate::proto::messages::kvs_request::KvsRequestKind;
use crate::proto::messages::kvs_response::ResponseResult;
use crate::proto::messages::{KvsRequest, KvsResponse};
use crate::{Error, ErrorKind, KvsEngine, KvsEngineVariant, Result};

/// Implementation of a server that listens for client requests, and performs the received commands
/// on the underlying key-value storage engine
#[allow(missing_debug_implementations)]
pub struct KvsServer {
    kvs_engine: Box<dyn KvsEngine>,
}

impl KvsServer {
    /// Create a new key-value store client.
    pub fn new(kvs_engine: Box<dyn KvsEngine>) -> Self {
        Self { kvs_engine }
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
