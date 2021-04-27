use bb03::Result;
use std::io::{Read, BufRead, BufReader};
use std::net::{SocketAddr, TcpListener};

fn main() -> Result<()> {
    let addr_local = SocketAddr::from(bb03::TEST_ADDR);
    let listener = TcpListener::bind(addr_local)?;

    let (stream, addr_remote) = listener.accept()?;
    println!("Establish new connection with {}", addr_remote);
    let mut stream_reader = BufReader::new(stream);

    let mut command_array_len_buf = Vec::new();
    stream_reader.consume(1);
    stream_reader.read_until(b'\r', &mut command_array_len_buf)?;
    stream_reader.consume(2);

    let command_array_len = String::from_utf8_lossy(&command_array_len_buf).parse().unwrap();
    let mut command_items: Vec<String> = Vec::with_capacity(command_array_len);

    for _ in 0..command_array_len {
        let mut command_item_len_buf = Vec::new();
        stream_reader.consume(1);
        stream_reader.read_until(b'\r', &mut command_item_len_buf)?;
        stream_reader.consume(2);

        let command_item_len = String::from_utf8_lossy(&command_array_len_buf).parse().unwrap();
        let mut command_item_buf = Vec::with_capacity(command_item_len);
        stream_reader.read_exact(&mut command_item_buf)?;

        command_items.push(String::from_utf8_lossy(&command_item_buf).into())
    }

    println!("{:?}", command_items);
    Ok(())
}
