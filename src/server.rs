use std::sync::Mutex;
use std::time::Duration;

use tokio;
use tonic::{transport::Server, Request, Response, Status};

use datastore::datastore_server::{Datastore, DatastoreServer};
use datastore::{GetRequest, GetResponse, SetRequest, SetResponse};
use rs_datastore::nestedmap::options::SetOptions;
use rs_datastore::nestedmap::NestedMap;

pub mod datastore {
    tonic::include_proto!("nestedmap");
}

#[derive(Debug)]
pub struct MyDatastore {
    map: Mutex<NestedMap>,
}

impl MyDatastore {
    pub fn new(max_history: usize) -> Self {
        MyDatastore {
            map: Mutex::new(NestedMap::new(max_history)),
        }
    }
}

#[tonic::async_trait]
impl Datastore for MyDatastore {
    async fn get(
        &self,
        request: tonic::Request<GetRequest>,
    ) -> Result<tonic::Response<GetResponse>, tonic::Status> {
        let keys = request.into_inner().keys;
        let map = self.map.lock().unwrap(); // Acquire the lock

        match map.get(&keys) {
            Some(item) => {
                let reply = GetResponse {
                    item: item.value.clone(), // Extracting the Vec<u8> directly from Item
                };
                Ok(tonic::Response::new(reply))
            }
            None => Err(tonic::Status::not_found("Key not found")),
        }
    }

    async fn set(
        &self,
        request: tonic::Request<SetRequest>,
    ) -> Result<tonic::Response<SetResponse>, tonic::Status> {
        let req = request.into_inner();
        let mut map = self.map.lock().unwrap(); // Acquire the lock for mutable access

        let options = req.options.map(|opts| SetOptions {
            preserve_history: opts.preserve_history,
            ttl: std::time::Duration::from_secs(opts.ttl as u64),
        });

        map.set(&req.keys, &req.value, options);

        let reply = SetResponse { success: true };
        Ok(tonic::Response::new(reply))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let my_datastore = MyDatastore::new(3);

    Server::builder()
        .add_service(DatastoreServer::new(my_datastore))
        .serve(addr)
        .await?;

    Ok(())
}
