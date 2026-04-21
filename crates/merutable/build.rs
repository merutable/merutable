// Issue #28 Phase 1: generate protobuf bindings for the internal
// catalog manifest format. Keeps the generated code out of git —
// `prost-build` writes into `OUT_DIR` at compile time.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/manifest.proto");
    prost_build::Config::new()
        // Optional: derive serde for cases where we want to dump a
        // generated message to JSON for debugging. Skipped in Phase 1
        // to keep the generated-code dependency surface tight.
        .compile_protos(&["proto/manifest.proto"], &["proto"])?;
    Ok(())
}
