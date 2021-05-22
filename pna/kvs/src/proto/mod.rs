//! Simple client-server network protocol that uses protocol buffer over TCP.
mod messages {
    include!(concat!(env!("OUT_DIR"), "/kvs.proto.messages.rs"));
}
mod client;
mod server;

pub use client::KvsClient;
pub use server::KvsServer;
