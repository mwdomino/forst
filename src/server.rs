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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    println!("Starting gRPC server with configuration: ");
    println!("\t Listen IP: {}", args.listen_ip);
    println!("\t Port: {}", args.port);
    println!("\t Max history: {}", args.max_history);

    let listener = TcpListener::bind(SocketAddr::new(args.listen_ip, args.port)).await?;
    let datastore = Arc::new(Datastore::new(args.max_history));
    service::serve(listener, datastore, async {
        signal::ctrl_c().await.expect("failed to listen for event");
    })
    .await?;

    println!("\nShutting down gracefully...");
    Ok(())
}
