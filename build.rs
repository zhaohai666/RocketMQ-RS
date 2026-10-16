fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protos = [
        "proto/apache/rocketmq/v2/definition.proto",
        "proto/apache/rocketmq/v2/service.proto",
        "proto/apache/rocketmq/v2/admin.proto",
    ];

    // Rerun the codegen when any proto file (or the include root) changes.
    for proto in &protos {
        println!("cargo:rerun-if-changed={proto}");
    }
    println!("cargo:rerun-if-changed=proto/apache/rocketmq/v2");

    // `protoc` (v25.3 on this machine) provides the well-known `google/protobuf/*.proto`
    // imports via its built-in include path, so we only need to point at our `proto/` root.
    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(&protos, &["proto"])?;

    Ok(())
}
