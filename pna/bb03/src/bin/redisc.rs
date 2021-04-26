use std::io::{BufWriter, BufReader};
use std::net::{SocketAddr, TcpListener, TcpStream};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr_local = SocketAddr::from(bb03::TEST_ADDR);
    let stream = TcpStream::connect(addr_local)?;
    Ok(())
}
