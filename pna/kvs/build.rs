fn main() {
    prost_build::compile_protos(&["src/network.proto"], &["src/"]).unwrap();
}
