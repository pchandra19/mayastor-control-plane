fn main() {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/node-service.proto"], &["proto"])
        .expect("node grpc service protobuf compilation failed");
}
