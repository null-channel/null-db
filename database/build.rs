fn main() {
    tonic_build::configure()
        .compile(&["proto/message.proto"], &["proto"])
        .unwrap();
}
