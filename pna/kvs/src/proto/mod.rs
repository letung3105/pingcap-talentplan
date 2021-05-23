//! Simple client-server network protocol that uses protocol buffer over TCP.
mod messages {
    include!(concat!(env!("OUT_DIR"), "/kvs.proto.messages.rs"));

    use crate::proto::messages::kvs_request::KvsRequestKind;

    impl KvsRequestKind {
        pub fn as_str(&self) -> &'static str {
            match *self {
                Self::Set => "set",
                Self::Get => "get",
                Self::Remove => "remove",
            }
        }
    }
}
mod client;
mod server;

pub use client::KvsClient;
pub use server::KvsServer;
