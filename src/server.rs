use tonic::transport::Server;
use std::net::SocketAddr;
use tokio::net::TcpListener;

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

use tokio::runtime::Builder;

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
        println!("SET CALLED");
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

async fn serve(_: usize) {
    println!("Called serve");
    let addr: std::net::SocketAddr = "0.0.0.0:7777".parse().unwrap();
    let sock = socket2::Socket::new(
        match addr {
            SocketAddr::V4(_) => socket2::Domain::IPV4,
            SocketAddr::V6(_) => socket2::Domain::IPV6,
        },
        socket2::Type::STREAM,
        None,
    )
    .unwrap();

    sock.set_reuse_address(true).unwrap();
    sock.set_reuse_port(true).unwrap();
    sock.set_nonblocking(true).unwrap();
    sock.bind(&addr.into()).unwrap();
    sock.listen(8192).unwrap();

    let incoming =
        tokio_stream::wrappers::TcpListenerStream::new(TcpListener::from_std(sock.into()).unwrap());

    let my_datastore = MyDatastore::new(3);

    Server::builder()
        .add_service(DatastoreServer::new(my_datastore))
        .serve_with_incoming(incoming)
        .await
        .unwrap();
    println!("Serving on 7777");
}

fn main() {
    let mut handlers = Vec::new();
    println!("Launching!");
    for i in 0..2 {
        let h = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(serve(i));
        });
        handlers.push(h);
    }

    for h in handlers {
        h.join().unwrap();
    }
}
