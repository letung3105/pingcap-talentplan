use assert_cmd::prelude::*;
use predicates::ord::eq;
use predicates::prelude::PredicateStrExt;
use predicates::str::contains;
use std::process::Command;

const CLIENT_EXECUTABLE_NAME: &str = "bluisc";

// `bluisc` with no args should exit with a non-zero code.
#[test]
fn cli_no_args() {
    Command::cargo_bin(CLIENT_EXECUTABLE_NAME)
        .unwrap()
        .assert()
        .failure();
}

// `bluisc -V` should print the version
#[test]
fn cli_version() {
    Command::cargo_bin(CLIENT_EXECUTABLE_NAME)
        .unwrap()
        .args(&["-V"])
        .assert()
        .stdout(contains(env!("CARGO_PKG_VERSION")));
}

#[test]
fn cli_ping_empty() {
    Command::cargo_bin(CLIENT_EXECUTABLE_NAME)
        .unwrap()
        .args(&["ping"])
        .assert()
        .success()
        .stdout(eq("Ping message").trim());
}

// `bluisc ping <MESSAGE>` should print the RESP server's reply, where the reply is the message that was passed as argument
#[test]
fn cli_ping_with_message() {
    Command::cargo_bin(CLIENT_EXECUTABLE_NAME)
        .unwrap()
        .args(&["ping", "Ping message"])
        .assert()
        .success()
        .stdout(eq("Ping message").trim());
}
