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

fn main() {
    let mv = Move {
        direction: Direction::Up,
        steps: 3,
    };
    println!("`mv` original: {:?}", mv);

    // JSON format
    {
        let f_json = std::fs::File::create("./test.json").unwrap();
        serde_json::to_writer(f_json, &mv).unwrap();

        let f_json = std::fs::File::open("./test.json").unwrap();
        let mv: Move = serde_json::from_reader(f_json).unwrap();
        println!("\n`mv` from JSON: {:?}", mv);

        let mv_json = serde_json::to_vec(&mv).unwrap();
        println!("`mv` JSON to vec: {:?}", &mv_json);
        println!(
            "`mv` JSON to string: {:?}",
            String::from_utf8_lossy(&mv_json)
        );
    }

    // RON format
    {
        let f_ron = std::fs::File::create("./test.ron").unwrap();
        ron::ser::to_writer(f_ron, &mv).unwrap();

        let f_ron = std::fs::File::open("./test.ron").unwrap();
        let mv: Move = ron::de::from_reader(f_ron).unwrap();
        println!("\n`mv` from RON: {:?}", mv);

        let mut mv_ron = Vec::new();
        ron::ser::to_writer(&mut mv_ron, &mv).unwrap();
        println!("`mv` RON to vec: {:?}", &mv_ron);
        println!(
            "`mv` RON to string: `{:?}",
            String::from_utf8_lossy(&mv_ron)
        );
    }

    // BSON format
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

        println!("\n`moves` from BSON: {:?}", moves);
    }
}
