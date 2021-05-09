//! Simple client-server network protocol that uses protocol buffer over TCP.
mod client;
mod server;
mod messages {
    include!(concat!(env!("OUT_DIR"), "/kvs.proto.messages.rs"));
}

pub use client::*;
pub use server::*;
