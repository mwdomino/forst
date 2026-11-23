use std::net::{IpAddr, SocketAddr};

use mimalloc::MiMalloc;

use clap::{value_parser, Parser};

use tokio::signal;
use tokio::sync::oneshot;
use tonic::transport::Server;

use datastore::datastore_server::{Datastore as DatastoreTrait, DatastoreServer};
use datastore::{
    DeleteAtIndexRequest, DeleteAtIndexResponse, DeleteRequest, DeleteResponse, GetRequest,
    GetResponse, Item, QueryRequest, QueryResponse, SetRequest, SetResponse,
};
use rs_datastore::datastore::Datastore;
use rs_datastore::nestedmap::options::{GetOptions, SetOptions};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

pub mod datastore {
    tonic::include_proto!("datastore");
}

#[derive(Debug)]
pub struct MyDatastore {
    datastore: Datastore,
}

impl MyDatastore {
    pub fn new(max_history: usize) -> Self {
        MyDatastore {
            datastore: Datastore::new(max_history),
        }
    }
}

#[tonic::async_trait]
impl DatastoreTrait for MyDatastore {
    async fn get(
        &self,
        request: tonic::Request<GetRequest>,
    ) -> Result<tonic::Response<GetResponse>, tonic::Status> {
        let key = request.into_inner().key;

        match self.datastore.get(&key) {
            Some(item) => {
                let reply = GetResponse {
                    item: Some(Item {
                        key: item.key,
                        value: item.value,
                    }),
                };

                Ok(tonic::Response::new(reply))
            }
            None => Ok(tonic::Response::new(GetResponse { item: None })),
        }
    }

    async fn set(
        &self,
        request: tonic::Request<SetRequest>,
    ) -> Result<tonic::Response<SetResponse>, tonic::Status> {
        let req = request.into_inner();

        let options = match req.options {
            Some(opts) if opts.ttl < 0 => {
                return Err(tonic::Status::invalid_argument("ttl must not be negative"))
            }
            Some(opts) => Some(SetOptions {
                preserve_history: opts.preserve_history,
                ttl: std::time::Duration::from_secs(opts.ttl as u64),
            }),
            None => None,
        };

        self.datastore.set(req.key, req.value, options);

        let reply = SetResponse { success: true };
        Ok(tonic::Response::new(reply))
    }

    async fn query(
        &self,
        request: tonic::Request<QueryRequest>,
    ) -> Result<tonic::Response<QueryResponse>, tonic::Status> {
        let inner = request.into_inner();
        let key = inner.key;

        let options = inner
            .options
            .and_then(|opts| opts.history_count)
            .map(|count| GetOptions {
                history_count: count.max(0) as usize,
            });

        let items = self.datastore.query(&key, options);

        // Construct the response from the items
        let reply = QueryResponse {
            items: items
                .into_iter()
                .map(|item| Item {
                    key: item.key,
                    value: item.value,
                })
                .collect(),
        };

        Ok(tonic::Response::new(reply))
    }

    async fn delete(
        &self,
        request: tonic::Request<DeleteRequest>,
    ) -> Result<tonic::Response<DeleteResponse>, tonic::Status> {
        let success = self.datastore.delete(&request.into_inner().key);
        Ok(tonic::Response::new(DeleteResponse { success }))
    }

    async fn delete_at_index(
        &self,
        request: tonic::Request<DeleteAtIndexRequest>,
    ) -> Result<tonic::Response<DeleteAtIndexResponse>, tonic::Status> {
        let req = request.into_inner();
        let index = usize::try_from(req.index)
            .map_err(|_| tonic::Status::invalid_argument("index must not be negative"))?;
        let success = self.datastore.delete_at_index(&req.key, index);
        Ok(tonic::Response::new(DeleteAtIndexResponse { success }))
    }
}

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

    // Set up a channel to receive a shutdown signal
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    // Spawn a task to handle signals
    tokio::spawn(async move {
        signal::ctrl_c().await.expect("failed to listen for event");
        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");
    });

    let addr = SocketAddr::new(args.listen_ip, args.port);
    let my_datastore = MyDatastore::new(args.max_history);

    println!("Starting gRPC server with configuration: ");
    println!("\t Listen IP: {}", args.listen_ip);
    println!("\t Port: {}", args.port);
    println!("\t Max history: {}", args.max_history);

    let server = Server::builder()
        .tcp_nodelay(true)
        .add_service(DatastoreServer::new(my_datastore))
        .serve_with_shutdown(addr, async {
            shutdown_rx.await.ok();
        });

    server.await?;

    println!("\nShutting down gracefully...");

    Ok(())
}
