use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use clap::{value_parser, Parser};
use mimalloc::MiMalloc;
use tokio::net::TcpListener;
use tokio::signal;

use rs_datastore::datastore::Datastore;
use rs_datastore::service;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    // IP to listen on
    #[arg(short, long, default_value = "0.0.0.0", value_parser = value_parser!(IpAddr))]
    listen_ip: IpAddr,

    // Listen port
    #[arg(short, long, default_value_t = 7777)]
    port: u16,

    // Max history for datastore
    #[arg(short, long, default_value_t = 5)]
    max_history: usize,

    // Number of shards; 0 means one per CPU
    #[arg(long, default_value_t = 0)]
    shards: usize,

    // Key segments that pick a key's shard; keys sharing them share a shard.
    // 5 fits net.company.datacenter.rack.switchname.<path>
    #[arg(long, default_value_t = 5)]
    shard_depth: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    println!("Starting gRPC server with configuration: ");
    println!("\t Listen IP: {}", args.listen_ip);
    println!("\t Port: {}", args.port);
    let shards = match args.shards {
        0 => std::thread::available_parallelism().map_or(1, |n| n.get()),
        n => n,
    };
    println!("\t Max history: {}", args.max_history);
    println!("\t Shards: {} (depth {})", shards, args.shard_depth);

    let listener = TcpListener::bind(SocketAddr::new(args.listen_ip, args.port)).await?;
    let datastore = Arc::new(Datastore::with_shards(
        args.max_history,
        shards,
        args.shard_depth,
    ));
    service::serve(listener, datastore, async {
        signal::ctrl_c().await.expect("failed to listen for event");
    })
    .await?;

    println!("\nShutting down gracefully...");
    Ok(())
}
