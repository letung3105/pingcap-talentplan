use std::io::{Read, Write};
use std::ops::Deref;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Move {
    direction: Direction,
    steps: i32,
}

impl Move {
    fn new(direction: Direction, steps: i32) -> Self {
        Self { direction, steps }
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum Direction {
    Up,
    Down,
    Left,
    Right,
}

#[derive(Debug, Default)]
struct BytesBuffer {
    inner: Vec<u8>,
    cursor: usize,
}

impl Deref for BytesBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Write for BytesBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let bytes_write = buf.len();
        self.inner.extend_from_slice(buf);
        self.cursor += bytes_write;
        Ok(bytes_write)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.cursor = 0;
        Ok(())
    }
}

impl Read for BytesBuffer {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut bytes_read = 0;
        self.inner
            .iter_mut()
            .skip(self.cursor)
            .zip(buf.iter_mut())
            .for_each(|(b_inner, b_buf)| {
                *b_buf = *b_inner;
                bytes_read += 1;
            });
        self.cursor += bytes_read;
        Ok(bytes_read)
    }
}

fn main() {
    let mv = Move {
        direction: Direction::Up,
        steps: 3,
    };
    println!("`mv` original: {:?}", &mv);

    // JSON format
    {
        let f_json = std::fs::File::create("./test.json").unwrap();
        serde_json::to_writer(f_json, &mv).unwrap();

        let f_json = std::fs::File::open("./test.json").unwrap();
        let mv: Move = serde_json::from_reader(f_json).unwrap();
        println!("\n`mv` from JSON file: {:?}", &mv);

        let mv_json_buf = serde_json::to_vec(&mv).unwrap();
        println!("`mv` JSON bytes buffer: {:?}", &mv_json_buf);
        println!(
            "`mv` JSON bytes as string: {:?}",
            String::from_utf8_lossy(&mv_json_buf)
        );
    }

    // RON format
    {
        let f_ron = std::fs::File::create("./test.ron").unwrap();
        ron::ser::to_writer(f_ron, &mv).unwrap();

        let f_ron = std::fs::File::open("./test.ron").unwrap();
        let mv: Move = ron::de::from_reader(f_ron).unwrap();
        println!("\n`mv` from RON file: {:?}", &mv);

        let mut mv_ron_buf = Vec::new();
        ron::ser::to_writer(&mut mv_ron_buf, &mv).unwrap();
        println!("`mv` RON bytes buffer: {:?}", &mv_ron_buf);
        println!(
            "`mv` RON bytes as string: `{:?}",
            String::from_utf8_lossy(&mv_ron_buf)
        );
    }

    // BSON format
    {
        let mut f_bson = std::fs::File::create("./test.bson").unwrap();
        let mv_bson_doc = bson::to_document(&mv).unwrap();
        mv_bson_doc.to_writer(&mut f_bson).unwrap();

        let mut f_bson = std::fs::File::open("./test.bson").unwrap();
        let mv_bson_doc = bson::Document::from_reader(&mut f_bson).unwrap();
        let mv: Move = bson::from_document(mv_bson_doc).unwrap();
        println!("\n`mv` from BSON file: {:?}", &mv);

        let mut mv_bson_buf = Vec::new();
        let mv_bson_doc = bson::to_document(&mv).unwrap();
        mv_bson_doc.to_writer(&mut mv_bson_buf).unwrap();
        println!("`mv` BSON bytes buffer: {:?}", &mv_bson_buf);
        println!(
            "`mv` BSON bytes as string: `{:?}",
            String::from_utf8_lossy(&mv_bson_buf)
        );
    }

    // BSON format many to file
    {
        fn serialize_moves<Writer: std::io::Write>(moves: &[Move], w: &mut Writer) {
            moves
                .iter()
                .map(|mv| bson::to_document(mv).unwrap())
                .for_each(|mv_bson_doc| mv_bson_doc.to_writer(w).unwrap());
        }

        fn deserialize_moves<Reader: std::io::Read>(moves: &mut [Move], r: &mut Reader, n: usize) {
            (0..n)
                .map(|_| bson::Document::from_reader(r).unwrap())
                .enumerate()
                .for_each(|(i, mv_bson_doc)| moves[i] = bson::from_document(mv_bson_doc).unwrap());
        }

        // Serialize/Deserialize to/from file
        {
            let moves = [
                Move::new(Direction::Up, 1111),
                Move::new(Direction::Down, 11),
                Move::new(Direction::Left, 11),
                Move::new(Direction::Right, 1),
            ];
            let mut f_bson = std::fs::File::create("./test.bson").unwrap();
            serialize_moves(&moves, &mut f_bson);

            let mut moves = [
                Move::new(Direction::Up, -1),
                Move::new(Direction::Up, -1),
                Move::new(Direction::Up, -1),
                Move::new(Direction::Up, -1),
            ];
            let mut f_bson = std::fs::File::open("./test.bson").unwrap();
            deserialize_moves(&mut moves, &mut f_bson, 4);
            println!("\n`moves` from BSON file: {:?}", &moves);
        }

        // Serialize/Deserialize to/from vec
        {
            let moves = [
                Move::new(Direction::Up, 1111),
                Move::new(Direction::Down, 11),
                Move::new(Direction::Left, 11),
                Move::new(Direction::Right, 1),
            ];

            let mut moves_bson_buf = BytesBuffer::default();
            serialize_moves(&moves, &mut moves_bson_buf);
            println!("\n`moves` BSON bytes buffer: {:?}", &moves_bson_buf);
            println!(
                "\n`moves` BSON bytes as string: {:?}",
                String::from_utf8_lossy(&moves_bson_buf)
            );

            let mut moves = [
                Move::new(Direction::Up, -1),
                Move::new(Direction::Up, -1),
                Move::new(Direction::Up, -1),
                Move::new(Direction::Up, -1),
            ];
            moves_bson_buf.flush().unwrap();
            deserialize_moves(&mut moves, &mut moves_bson_buf, 4);
            println!("\n`moves` from BSON bytes buffer: {:?}", &moves);
        }
    }
}
