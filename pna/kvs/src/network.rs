//! Providing network API for interacting with the key-value store implementation

use crate::{Error, ErrorKind, KvsEngineVariant, Result};
use bytes::{BufMut, BytesMut};
use prost::Message;
use std::io::{BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;

mod proto {
    #![allow(missing_docs)]
    include!(concat!(env!("OUT_DIR"), "/kvs.network.proto.rs"));
}

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
        let req = proto::KvsRequest {
            kind: proto::kvs_request::KvsRequestKind::Set as i32,
            key,
            value,
        };

        let res = self.make_request(req)?;
        match res.response_result {
            Some(result) => match result {
                proto::kvs_response::ResponseResult::ErrorMessage(msg) => {
                    Err(Error::new(ErrorKind::ServerError(msg)))
                }
                _ => Err(Error::new(ErrorKind::InvalidKvsResponse)),
            },
            None => Ok(()),
        }
    }

    /// Send get command request to the key-val store's server
    pub fn get_req(&self, key: String) -> Result<Option<String>> {
        let req = proto::KvsRequest {
            kind: proto::kvs_request::KvsRequestKind::Get as i32,
            key,
            value: String::default(),
        };

        let res = self.make_request(req)?;
        match res.response_result {
            Some(result) => match result {
                proto::kvs_response::ResponseResult::ErrorMessage(msg) => {
                    Err(Error::new(ErrorKind::ServerError(msg)))
                }
                proto::kvs_response::ResponseResult::GetCommandValue(value) => Ok(Some(value)),
            },
            None => Ok(None),
        }
    }

    /// Send remove command request to the key-val store's server
    pub fn remove_req(&self, key: String) -> Result<()> {
        let req = proto::KvsRequest {
            kind: proto::kvs_request::KvsRequestKind::Remove as i32,
            key,
            value: String::default(),
        };

        let res = self.make_request(req)?;
        match res.response_result {
            Some(result) => match result {
                proto::kvs_response::ResponseResult::ErrorMessage(msg) => {
                    Err(Error::new(ErrorKind::ServerError(msg)))
                }
                _ => Err(Error::new(ErrorKind::InvalidKvsResponse)),
            },
            None => Ok(()),
        }
    }

    fn make_request(&self, req: proto::KvsRequest) -> Result<proto::KvsResponse> {
        let mut request_bytes = vec![];
        req.encode_length_delimited(&mut request_bytes)?;
        let mut stream = TcpStream::connect(self.server_addr)?;
        stream.write_all(&request_bytes)?;

        let mut len_delim_bytes = [0u8; 10];
        let mut msg_bytes = BytesMut::new();
        let mut stream_reader = BufReader::new(stream);

        loop {
            stream_reader.read(&mut len_delim_bytes)?;
            msg_bytes.put_slice(&len_delim_bytes);

            match prost::decode_length_delimiter(msg_bytes.as_ref()) {
                Ok(len) => {
                    let len_delim_bytes_len = prost::length_delimiter_len(len);
                    let n_remaining = len - (msg_bytes.len() - len_delim_bytes_len);

                    let mut msg_bytes_remaining = vec![0u8; n_remaining];
                    stream_reader.read_exact(&mut msg_bytes_remaining)?;
                    msg_bytes.put_slice(&msg_bytes_remaining);

                    return Ok(proto::KvsResponse::decode(
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
    engine_variant: KvsEngineVariant,
    data_path: PathBuf,
}

impl KvsServer {
    /// Create a new key-value store client.
    pub fn new<P>(engine_variant: KvsEngineVariant, data_path: P) -> Self
    where
        P: Into<PathBuf>,
    {
        // let listener = TcpListener::bind("127.0.0.1:8080").unwrap();
        // for stream in listener.incoming() {
        //     let mut stream = stream.unwrap();
        //     let mut sreader = BufReader::new(stream.try_clone().unwrap());
        //     println!("Client accepted!");

        //     let mut len_delim_buf = [0u8; 10];
        //     let mut msg_len_delim = BytesMut::new();

        //     loop {
        //         sreader.read(&mut len_delim_buf).unwrap();
        //         msg_len_delim.put_slice(&len_delim_buf);
        //         println!("-- Buffer {:?}", msg_len_delim);

        //         match prost::decode_length_delimiter(msg_len_delim.as_ref()) {
        //             Ok(len) => {
        //                 println!("-- Length delimiter: {}", len);
        //                 let len_delim_length = prost::length_delimiter_len(len);
        //                 let n_remaining = len - (msg_len_delim.len() - len_delim_length);
        //                 let mut msg_remaining = vec![0u8; n_remaining];
        //                 sreader.read_exact(&mut msg_remaining).unwrap();
        //                 msg_len_delim.put_slice(&msg_remaining);
        //                 println!("-- Buffer {:?}", msg_len_delim);

        //                 let msg =
        //                     CommandRequest::decode(msg_len_delim.split_off(len_delim_length)).unwrap();
        //                 println!("-- Parsed: {:?}", msg);

        //                 match CommandName::from_i32(msg.name) {
        //                     Some(CommandName::Set) => {
        //                         let resp = CommandResponse {
        //                             command_response_body: None,
        //                         };
        //                         let mut buf = vec![];
        //                         resp.encode_length_delimited(&mut buf).unwrap();
        //                         stream.write_all(&buf).unwrap();
        //                         println!("Sending {:?}", resp);
        //                         println!("-- Buffer {:?}", buf);
        //                     }
        //                     Some(CommandName::Get) => {
        //                         let resp = CommandResponse {
        //                             command_response_body: Some(CommandResponseBody::GetResp(
        //                                 GetCommandResponse {
        //                                     value: "Hey there!".to_string(),
        //                                 },
        //                             )),
        //                         };
        //                         let mut buf = vec![];
        //                         resp.encode_length_delimited(&mut buf).unwrap();
        //                         stream.write_all(&buf).unwrap();
        //                         println!("Sending {:?}", resp);
        //                         println!("-- Buffer {:?}", buf);
        //                     }
        //                     Some(CommandName::Remove) => {
        //                         let resp = CommandResponse {
        //                             command_response_body: None,
        //                         };
        //                         let mut buf = vec![];
        //                         resp.encode_length_delimited(&mut buf).unwrap();
        //                         stream.write_all(&buf).unwrap();
        //                         println!("Sending {:?}", resp);
        //                         println!("-- Buffer {:?}", buf);
        //                     }
        //                     None => {}
        //                 }

        //                 break;
        //             }
        //             Err(err) => {
        //                 if msg_len_delim.len() > 10 {
        //                     eprintln!("{}", err);
        //                     break;
        //                 }
        //             }
        //         }
        //     }
        // }

        let data_path = data_path.into();
        Self {
            engine_variant,
            data_path,
        }
    }

    /// Starting accepting requests on the given IP address and modify the key-value store
    /// based on the received command
    pub fn serve<A>(&self, addr: A) -> Result<()>
    where
        A: Into<SocketAddr>,
    {
        todo!()
    }
}
