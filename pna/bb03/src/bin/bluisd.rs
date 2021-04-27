use std::net::{SocketAddr, TcpListener};
use bb03::Result;

fn main() -> Result<()> {
    let addr_local = SocketAddr::from(bb03::TEST_ADDR);
    let listener = TcpListener::bind(addr_local)?;

    let (_stream, addr_remote) = listener.accept()?;
    println!("Establish new connection with {}", addr_remote);

    Ok(())
}
