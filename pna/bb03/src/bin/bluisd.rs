use bb03::Result;
use std::io::{BufRead, BufReader, Read};
use std::net::{SocketAddr, TcpListener};

fn main() -> Result<()> {
    let addr_local = SocketAddr::from(bb03::TEST_ADDR);
    let listener = TcpListener::bind(addr_local)?;
    let (stream, addr_remote) = listener.accept()?;
    println!("Establish new connection with {}", addr_remote);

    let mut stream_reader = BufReader::new(stream);

    let mut command_array_len_buf = vec![];
    stream_reader.consume(1);
    stream_reader.read_until(b'\r', &mut command_array_len_buf)?;
    stream_reader.consume(2);
    let command_array_len = String::from_utf8_lossy(&command_array_len_buf)
        .parse()
        .unwrap();
    println!("Command size: {:?}", command_array_len);

    let mut command_items: Vec<String> = Vec::with_capacity(command_array_len);
    for _ in 0..command_array_len {
        let mut command_item_len_buf = Vec::new();
        stream_reader.consume(1);
        stream_reader.read_until(b'\r', &mut command_item_len_buf)?;
        stream_reader.consume(2);
        let command_item_len = String::from_utf8_lossy(&command_array_len_buf)
            .parse()
            .unwrap();
        println!("Item size: {:?}", command_item_len);

        let mut command_item_buf = vec![0u8; command_item_len];
        stream_reader.read_exact(&mut command_item_buf)?;
        println!("Item (bytes): {:?}", command_item_buf);

        let command_item_string = String::from_utf8(command_item_buf).unwrap();
        println!("Item (string): {:?}", command_item_string);
        command_items.push(command_item_string);
    }

    println!("{:?}", command_items);
    Ok(())
}
