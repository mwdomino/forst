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

        match self.datastore.get(&key).await {
            Some(item) => {
                let reply = GetResponse {
                    item: Some(Item {
                        key: item.key.clone(),
                        value: item.value.clone(),
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

        let options = req.options.map(|opts| SetOptions {
            preserve_history: opts.preserve_history,
            ttl: std::time::Duration::from_secs(opts.ttl as u64),
        });

        self.datastore.set(req.key, &req.value, options).await;

        let reply = SetResponse { success: true };
        Ok(tonic::Response::new(reply))
    }

    async fn query(
        &self,
        request: tonic::Request<QueryRequest>,
    ) -> Result<tonic::Response<QueryResponse>, tonic::Status> {
        let inner = request.into_inner();
        let key = inner.key;

        let options = inner.options.map(|opts| GetOptions {
            history_count: opts.history_count.map_or(0, |count| {
                if count >= 0 {
                    count as usize
                } else {
                    0
                }
            }),
        });

        let items = self.datastore.query(&key, options).await;

        if items.is_empty() {
            return Err(tonic::Status::not_found(
                "No items found for the given keys",
            ));
        }

        // Construct the response from the items
        let reply = QueryResponse {
            items: items
                .into_iter()
                .map(|item| Item {
                    key: item.key.clone(),
                    value: item.value.clone(),
                })
                .collect(),
        };

        Ok(tonic::Response::new(reply))
    }

    async fn delete(
        &self,
        request: tonic::Request<DeleteRequest>,
    ) -> Result<tonic::Response<DeleteResponse>, tonic::Status> {
        return Err(tonic::Status::not_found("Not implemented"));
    }

    async fn delete_at_index(
        &self,
        request: tonic::Request<DeleteAtIndexRequest>,
    ) -> Result<tonic::Response<DeleteAtIndexResponse>, tonic::Status> {
        return Err(tonic::Status::not_found("Not implemented"));
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Set up a channel to receive a shutdown signal
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    // Spawn a task to handle signals
    tokio::spawn(async move {
        signal::ctrl_c().await.expect("failed to listen for event");
        shutdown_tx.send(()).expect("failed to send shutdown signal");
    });

    let addr = "0.0.0.0:7777".parse()?;
    let my_datastore = MyDatastore::new(3);

    println!("Starting gRPC server on 0.0.0.0:7777");


    let server = Server::builder()
        .add_service(DatastoreServer::new(my_datastore))
        .serve_with_shutdown(addr, async {
            shutdown_rx.await.ok();
        });

    server.await?;

    println!("Shutting down gracefully...");

    Ok(())
}
