//! Providing network API for interacting with the key-value store implementation

use crate::proto::messages::kvs_request::KvsRequestKind;
use crate::proto::messages::kvs_response::ResponseResult;
use crate::proto::messages::{KvsRequest, KvsResponse};
use crate::thread_pool::ThreadPool;
use crate::{Error, ErrorKind, KvsEngine, Result};
use bytes::{BufMut, BytesMut};
use prost::Message;
use slog::Drain;
use std::io::{BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};

/// Implementation of a server that listens for client requests, and performs the received commands
/// on the underlying key-value storage engine
#[allow(missing_debug_implementations)]
pub struct KvsServer<E, P>
where
    E: KvsEngine,
    P: ThreadPool,
{
    logger: slog::Logger,
    kvs_engine: E,
    pool: P,
}

impl<E, P> KvsServer<E, P>
where
    E: KvsEngine,
    P: ThreadPool,
{
    /// Create a new key-value store server that uses the given engine
    pub fn new<L>(kvs_engine: E, pool: P, logger: L) -> Self
    where
        L: Into<Option<slog::Logger>>,
    {
        let logger = logger.into().unwrap_or({
            let decorator = slog_term::TermDecorator::new().build();
            let drain = slog_term::FullFormat::new(decorator).build().fuse();
            let drain = slog_async::Async::new(drain).build().fuse();
            slog::Logger::root(drain, o!())
        });

        Self {
            logger,
            kvs_engine,
            pool,
        }
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
            if let Err(_) = stream {
                continue;
            }

            let mut stream = stream.unwrap();
            let peer_addr = stream.peer_addr()?;
            let kvs_engine = self.kvs_engine.clone();
            let logger = self.logger.new(o!( "peer_addr" => peer_addr.to_string() ));

            self.pool.spawn(move || match stream.try_clone() {
                Ok(s) => {
                    if let Err(err) = Self::handle_client(kvs_engine, s) {
                        let res = KvsResponse {
                            response_result: Some(ResponseResult::ErrorMessage(err.to_string())),
                        };
                        let mut res_buf = vec![];
                        res.encode_length_delimited(&mut res_buf).unwrap();
                        stream.write_all(&res_buf).unwrap();
                    }
                }
                Err(err) => {
                    error!(logger, "Could not clone network stream"; "error" => err);
                }
            });
        }
        Ok(())
    }

    fn handle_client(kvs_engine: E, stream: TcpStream) -> Result<()> {
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
                    let req_kind = KvsRequestKind::from_i32(req.kind);
                    let res = match req_kind {
                        Some(KvsRequestKind::Set) => {
                            Self::handle_set(kvs_engine, stream, req.key, req.value)
                        }
                        Some(KvsRequestKind::Get) => Self::handle_get(kvs_engine, stream, req.key),
                        Some(KvsRequestKind::Remove) => {
                            Self::handle_remove(kvs_engine, stream, req.key)
                        }
                        None => {
                            return Err(Error::new(
                                ErrorKind::InvalidNetworkMessage,
                                "Expecting an operation in the request",
                            ))
                        }
                    };
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

    fn handle_set(kvs_engine: E, mut stream: TcpStream, key: String, value: String) -> Result<()> {
        kvs_engine.set(key, value)?;
        let res = KvsResponse {
            response_result: None,
        };
        let mut res_buf = vec![];
        res.encode_length_delimited(&mut res_buf)?;
        stream.write_all(&res_buf)?;
        Ok(())
    }

    fn handle_get(kvs_engine: E, mut stream: TcpStream, key: String) -> Result<()> {
        let value = kvs_engine.get(key)?;
        let res = KvsResponse {
            response_result: value.map(|val| ResponseResult::GetCommandValue(val)),
        };
        let mut res_buf = vec![];
        res.encode_length_delimited(&mut res_buf)?;
        stream.write_all(&res_buf)?;
        Ok(())
    }

    fn handle_remove(kvs_engine: E, mut stream: TcpStream, key: String) -> Result<()> {
        kvs_engine.remove(key)?;
        let res = KvsResponse {
            response_result: None,
        };
        let mut res_buf = vec![];
        res.encode_length_delimited(&mut res_buf)?;
        stream.write_all(&res_buf)?;
        Ok(())
    }
}
