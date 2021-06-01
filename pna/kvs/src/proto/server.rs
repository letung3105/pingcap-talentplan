//! Providing network API for interacting with the key-value store implementation

use crate::proto::messages::kvs_request::KvsRequestKind;
use crate::proto::messages::kvs_response::ResponseResult;
use crate::proto::messages::{KvsRequest, KvsResponse};
use crate::{Error, ErrorKind, KvsEngine, Result};
use bytes::{BufMut, BytesMut};
use prost::Message;
use slog::Drain;
use std::io::{BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};

/// Implementation of a server that listens for client requests, and performs the received commands
/// on the underlying key-value storage engine
#[allow(missing_debug_implementations)]
pub struct KvsServer<E>
where
    E: KvsEngine,
{
    logger: slog::Logger,
    kvs_engine: E,
}

impl<E> KvsServer<E>
where
    E: KvsEngine,
{
    /// Create a new key-value store server that uses the given engine
    pub fn new<L>(kvs_engine: E, logger: L) -> Self
    where
        L: Into<Option<slog::Logger>>,
    {
        let logger = logger.into().unwrap_or({
            let decorator = slog_term::TermDecorator::new().build();
            let drain = slog_term::FullFormat::new(decorator).build().fuse();
            let drain = slog_async::Async::new(drain).build().fuse();
            slog::Logger::root(drain, o!())
        });

        Self { logger, kvs_engine }
    }

    /// Starting accepting requests on the given IP address and modify the key-value store
    /// based on the received command
    pub fn serve<A>(&mut self, addr: A) -> Result<()>
    where
        A: Into<SocketAddr>,
    {
        let addr = addr.into();
        self.logger = self.logger.new(o!("addr" => addr.to_string()));

        info!(self.logger, "Starting key-value store server");
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            if let Ok(mut stream) = stream {
                if let Err(err) = self.handle_client(stream.try_clone()?) {
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
        let peer_addr = stream.peer_addr()?;

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
                    let req_kind = KvsRequestKind::from_i32(req.kind);
                    let res = match req_kind {
                        Some(KvsRequestKind::Set) => self.handle_set(stream, req.key, req.value),
                        Some(KvsRequestKind::Get) => self.handle_get(stream, req.key),
                        Some(KvsRequestKind::Remove) => self.handle_remove(stream, req.key),
                        None => {
                            return Err(Error::new(
                                ErrorKind::InvalidNetworkMessage,
                                "Expecting an operation in the request",
                            ))
                        }
                    };

                    if let Some(req_kind) = req_kind {
                        info!(self.logger,
                            "Handled client request";
                            "peer_addr" => peer_addr.to_string(),
                            "request" => req_kind.as_str());
                    }
                    return res;
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
