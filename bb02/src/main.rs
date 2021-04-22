use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Move {
    direction: Direction,
    steps: i32,
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
        let mut f_bson = std::fs::File::create("./test.bson").unwrap();
        let mv_bson_doc = bson::to_document(&mv).unwrap();
        mv_bson_doc.to_writer(&mut f_bson).unwrap();

        let mut f_bson = std::fs::File::open("./test.bson").unwrap();
        let mv_bson_doc = bson::Document::from_reader(&mut f_bson).unwrap();
        let mv: Move = bson::from_document(mv_bson_doc).unwrap();
        println!("\n`mv` from BSON: {:?}", mv);

        let mut mv_bson_vec = Vec::new();
        let mv_bson_doc = bson::to_document(&mv).unwrap();
        mv_bson_doc.to_writer(&mut mv_bson_vec).unwrap();
        println!("`mv` BSON to vec: {:?}", &mv_bson_vec);
        println!(
            "`mv` BSON to string: `{:?}",
            String::from_utf8_lossy(&mv_bson_vec)
        );
    }
}
