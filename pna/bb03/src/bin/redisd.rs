use std::net::{SocketAddr, TcpListener};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr_local = SocketAddr::from(bb03::TEST_ADDR);
    let listener = TcpListener::bind(addr_local)?;
    match listener.accept() {
        Ok((_stream, addr_remote)) => {
            println!("Establish new connection with {}", addr_remote);
        }
        Err(err) => {
            eprintln!("Error while accepting new TCP connection {}", err);
            std::process::exit(1);
        }
    }
    Ok(())
}
