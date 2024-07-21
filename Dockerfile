# Builder
FROM rust:1.79.0 as builder
RUN apt update && apt install -y protobuf-compiler
WORKDIR /app
COPY . .
RUN cargo build --bin server --release

# Runner
FROM gcr.io/distroless/cc-debian12:debug
COPY --from=builder /app/target/release/server /usr/local/bin/server
ENTRYPOINT ["/usr/local/bin/server"]
