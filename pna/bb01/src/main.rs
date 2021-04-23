extern crate dotenv;
#[macro_use]
extern crate dotenv_codegen;
#[macro_use]
extern crate clap;

fn main() {
    let matches = clap_app!(app =>
        (version: "0.1.0")
        (author: "Some O. <someone@me.com>")
        (about: "A gooo CLI program")
        (@arg INPUT: +required "Set the path to the input file")
        (@arg config: -c --config [FILE] +takes_value "Set a custom path to the config file")
        (@arg debug: -d ... "Set the level of debugging information")
        (@subcommand test =>
            (version: "0.1.0")
            (author: "Someone E. <someone_else@notme.com>")
            (about: "A CLI test")
            (@arg verbose: -v --verbose "Print the test information verbosely")
        )
    )
    .get_matches();

    println!("HOME={}", dotenv!("HOME"));
    println!("HOST={}", dotenv!("HOST"));
    println!("PORT={}", dotenv!("PORT"));

    let input = matches.value_of("INPUT").unwrap();
    println!("Using input file: {}", input);

    let config = matches.value_of("config").unwrap_or("default.conf");
    println!("Value for config: {}", config);

    // Vary the output based on how many times the user used the "debug" flag
    // (i.e. 'bb01 -d -d -d' or 'myprog -ddd' vs 'bb01 -d'
    match matches.occurrences_of("debug") {
        0 => println!("No debugging info"),
        1 => println!("Some debugging info"),
        2 => println!("Tons of debugging info"),
        3 | _ => println!("Don't be crazy"),
    }

    // You can handle information about subcommands by requesting their matches by name
    // (as below), requesting just the name used, or both at the same time
    if let Some(matches) = matches.subcommand_matches("test") {
        if matches.is_present("verbose") {
            println!("Printing verbose info...");
        } else {
            println!("Printing normally...");
        }
    }
}
