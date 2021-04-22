use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Move {
    direction: Direction,
    steps: u32,
}

#[derive(Debug, Serialize, Deserialize)]
enum Direction {
    Up,
    Down,
    Left,
    Right,
}

fn main() {
    let move_a = Move {
        direction: Direction::Up,
        steps: 3,
    };
    println!("`move_a` original: {:?}", move_a);

    // JSON format
    {
        let json_file = std::fs::File::create("./test.json").unwrap();
        serde_json::to_writer(json_file, &move_a).unwrap();

        let json_file = std::fs::File::open("./test.json").unwrap();
        let move_a: Move = serde_json::from_reader(json_file).unwrap();
        println!("\n`move_a` from JSON: {:?}", move_a);

        let move_a_json = serde_json::to_vec(&move_a).unwrap();
        println!("`move_a` JSON to vec: {:?}", &move_a_json);
        println!(
            "`move_a` JSON to string: {:?}",
            String::from_utf8_lossy(&move_a_json)
        );
    }

    // RON format
    {
        let ron_file = std::fs::File::create("./test.ron").unwrap();
        ron::ser::to_writer(ron_file, &move_a).unwrap();

        let ron_file = std::fs::File::open("./test.ron").unwrap();
        let move_a: Move = ron::de::from_reader(ron_file).unwrap();
        println!("\n`move_a` from RON: {:?}", move_a);

        let mut move_a_ron = Vec::new();
        ron::ser::to_writer(&mut move_a_ron, &move_a).unwrap();
        println!("`move_a` RON to vec: {:?}", &move_a_ron);
        println!(
            "`move_a` RON to string: `{:?}",
            String::from_utf8_lossy(&move_a_ron)
        );
    }
}
