fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Item values are shared Bytes, so events and responses do not copy them.
    // Request values stay Vec<u8>: zero-copy Bytes would keep each whole request buffer
    // alive for as long as any one of its values is stored.
    tonic_prost_build::configure()
        .bytes(".datastore.Item.value")
        .compile_protos(&["proto/datastore.proto"], &["proto"])?;
    Ok(())
}
