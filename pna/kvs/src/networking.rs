//! Module for handling network communication between client and server

use std::{
    io::{BufReader, BufWriter, Write},
    net::{SocketAddr, TcpListener, TcpStream},
};

use serde::{Deserialize, Serialize};
use slog::Drain;

use crate::{thread_pool::ThreadPool, Error, ErrorKind, KvsEngine, Result};

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Request {
    Set { key: String, value: String },
    Get { key: String },
    Remove { key: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum SetResponse {
    Ok,
    Err(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum GetResponse {
    Ok(Option<String>),
    Err(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum RemoveResponse {
    Ok,
    Err(String),
}

/// Client interface
pub trait KvsClient {
    /// Connect to the remote server
    fn connect<A>(addr: A) -> Result<Self>
    where
        Self: Sized,
        A: Into<SocketAddr>;
    /// Send set command
    fn set(&mut self, key: String, value: String) -> Result<()>;
    /// Send get command
    fn get(&mut self, key: String) -> Result<Option<String>>;
    /// Send remove command
    fn remove(&mut self, key: String) -> Result<()>;
}

/// Server interface
pub trait KvsServer {
    /// Start accepting requests on the given socket address
    fn serve<A>(&mut self, addr: A) -> Result<()>
    where
        A: Into<SocketAddr>;
}

/// Network client for JSON message
#[derive(Debug)]
pub struct JsonKvsClient {
    rstream: BufReader<TcpStream>,
    wstream: BufWriter<TcpStream>,
}

impl KvsClient for JsonKvsClient {
    /// Connect to the remote server at `addr` and return the client to it
    fn connect<A>(addr: A) -> Result<Self>
    where
        A: Into<SocketAddr>,
    {
        let wstream = TcpStream::connect(addr.into())?;
        let rstream = wstream.try_clone()?;
        Ok(Self {
            rstream: BufReader::new(rstream),
            wstream: BufWriter::new(wstream),
        })
    }

    fn set(&mut self, key: String, value: String) -> Result<()> {
        let set_request = Request::Set { key, value };
        serde_json::to_writer(&mut self.wstream, &set_request)?;
        self.wstream.flush()?;

        let set_response: SetResponse = serde_json::from_reader(&mut self.rstream)?;
        match set_response {
            SetResponse::Ok => Ok(()),
            SetResponse::Err(err) => Err(Error::new(ErrorKind::ServerError, err)),
        }
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        let get_request = Request::Get { key };
        serde_json::to_writer(&mut self.wstream, &get_request)?;
        self.wstream.flush()?;

        let get_response: GetResponse = serde_json::from_reader(&mut self.rstream)?;
        match get_response {
            GetResponse::Ok(val) => Ok(val),
            GetResponse::Err(err) => Err(Error::new(ErrorKind::ServerError, err)),
        }
    }

    fn remove(&mut self, key: String) -> Result<()> {
        let remove_request = Request::Remove { key };
        serde_json::to_writer(&mut self.wstream, &remove_request)?;
        self.wstream.flush()?;

        let remove_response: RemoveResponse = serde_json::from_reader(&mut self.rstream)?;
        match remove_response {
            RemoveResponse::Ok => Ok(()),
            RemoveResponse::Err(err) => Err(Error::new(ErrorKind::ServerError, err)),
        }
    }
}

/// Network server for JSON message
#[derive(Debug)]
pub struct JsonKvsServer<E, P>
where
    E: KvsEngine,
    P: ThreadPool,
{
    engine: E,
    pool: P,
    logger: slog::Logger,
}

impl<E, P> KvsServer for JsonKvsServer<E, P>
where
    E: KvsEngine,
    P: ThreadPool,
{
    fn serve<A>(&mut self, addr: A) -> Result<()>
    where
        A: Into<SocketAddr>,
    {
        let addr = addr.into();
        let logger = self.logger.new(o!("addr" => addr.to_string()));
        info!(logger, "Starting key-value store server");

        let tcp_listener = TcpListener::bind(addr)?;
        for stream in tcp_listener.incoming() {
            if let Err(err) = stream {
                error!(logger, "Could not connect TcpStream"; "error" => err);
                continue;
            }

            let stream = stream.unwrap();
            let kvs_engine = self.engine.clone();
            let logger = logger.new(o!( "peer_addr" => stream.peer_addr()?.to_string() ));
            info!(logger, "Peer connected.");

            self.pool.spawn(move || {
                if let Err(err) = Self::handle(kvs_engine, stream) {
                    error!(logger, "Could not handle client"; "error" => format!("{}", err));
                }
            });
        }
        Ok(())
    }
}

impl<E, P> JsonKvsServer<E, P>
where
    E: KvsEngine,
    P: ThreadPool,
{
    /// Create a new JSON server
    pub fn new<L>(engine: E, pool: P, logger: Option<L>) -> Self
    where
        L: Into<slog::Logger>,
    {
        let logger = logger.map(|l| l.into()).unwrap_or({
            // TODO: make default log config
            let decorator = slog_term::TermDecorator::new().build();
            let drain = slog_term::FullFormat::new(decorator).build().fuse();
            let drain = slog_async::Async::new(drain).build().fuse();
            slog::Logger::root(drain, o!())
        });

        Self {
            engine,
            pool,
            logger,
        }
    }

    fn handle(engine: E, stream: TcpStream) -> Result<()> {
        let mut wstream = BufWriter::new(stream.try_clone()?);
        let mut rstream = BufReader::new(stream);

        let request: Request = serde_json::from_reader(&mut rstream)?;
        match request {
            Request::Set { key, value } => 
            {
                let res = match engine.set(key, value) {
                    Ok(_) => SetResponse::Ok,
                    Err(err) => SetResponse::Err(format!("{}", err)),
                };
                serde_json::to_writer(&mut wstream, &res)
            },
            Request::Get { key } => {
                let res = match engine.get(key) {
                    Ok(v) => GetResponse::Ok(v),
                    Err(err) => GetResponse::Err(format!("{}", err)),
                };
                serde_json::to_writer(&mut wstream, &res)
            },
            Request::Remove { key } => {
                let res = match engine.remove(key) {
                    Ok(_) => RemoveResponse::Ok,
                    Err(err) => RemoveResponse::Err(format!("{}", err)),
                };
                serde_json::to_writer(&mut wstream, &res)
            }
        }?;

        Ok(())
    }
}
