use bb03::Result;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};

fn main() -> Result<()> {
    let addr_local = SocketAddr::from(bb03::TEST_ADDR);
    let listener = TcpListener::bind(addr_local)?;

    for stream in listener.incoming() {
        let stream = stream?;
        let addr_remote = stream.peer_addr()?;
        println!("\nCONNECTED {}", addr_remote);
        handle_request(stream)?;
        println!("TERMINATED {}\n", addr_remote);
    }

    Ok(())
}

fn handle_request(mut stream: TcpStream) -> Result<()> {
    let mut stream_reader = BufReader::new(stream.try_clone()?);

    let mut arr_len_buf = vec![];
    stream_reader.read_exact(&mut [0; 1])?;
    stream_reader.read_until(b'\r', &mut arr_len_buf)?;
    stream_reader.read_exact(&mut [0; 1])?;

    let arr_len_buf = match arr_len_buf.split_last() {
        None => Vec::new(),
        Some((_, until_last)) => Vec::from(until_last),
    };
    let arr_len = String::from_utf8(arr_len_buf).unwrap().parse().unwrap();
    println!("Array len: {:?}", arr_len);

    let mut command_items: Vec<String> = Vec::with_capacity(arr_len);
    for _ in 0..arr_len {
        let mut item_len_buf = Vec::new();
        stream_reader.read_exact(&mut [0; 1])?;
        stream_reader.read_until(b'\r', &mut item_len_buf)?;
        stream_reader.read_exact(&mut [0; 1])?;

        let item_len_buf = match item_len_buf.split_last() {
            None => Vec::new(),
            Some((_, until_last)) => Vec::from(until_last),
        };
        let item_len = String::from_utf8(item_len_buf).unwrap().parse().unwrap();
        println!("\tItem length: {:?}", item_len);

        let mut item_buf = vec![0u8; item_len];
        stream_reader.read_exact(&mut item_buf)?;
        stream_reader.read_exact(&mut [0; 2])?;
        println!("\tItem bytes: {:?}", item_buf);

        let item_string = String::from_utf8(item_buf).unwrap();
        println!("\tItem text: {:?}", item_string);
        println!("\t========");

        command_items.push(item_string);
    }

    let message = match command_items.get(1) {
        None => "PONG",
        Some(item) => item,
    };
    let mut packet = Vec::new();
    packet.extend_from_slice(format!("${}\r\n", message.len()).as_bytes());
    packet.extend_from_slice(format!("{}\r\n", message).as_bytes());

    println!("Encoded response: {:?}", packet);
    stream.write_all(&packet)?;

    Ok(())
}
