use std::sync::Mutex;

use tonic::transport::Server;

use datastore::datastore_server::{Datastore, DatastoreServer};
use datastore::{
    DeleteAtIndexRequest, DeleteAtIndexResponse, DeleteRequest, DeleteResponse, GetRequest,
    GetResponse, Item, QueryRequest, QueryResponse, SetRequest, SetResponse,
};
use rs_datastore::nestedmap::options::SetOptions;
use rs_datastore::nestedmap::NestedMap;

pub mod datastore {
    tonic::include_proto!("datastore");
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
        let keys = request.into_inner().key;
        let map = self.map.lock().unwrap(); // Acquire the lock
        match map.get(&keys) {
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
        let mut map = self.map.lock().unwrap(); // Acquire the lock for mutable access

        let options = req.options.map(|opts| SetOptions {
            preserve_history: opts.preserve_history,
            ttl: std::time::Duration::from_secs(opts.ttl as u64),
        });

        map.set(&req.key, &req.value, options);

        let reply = SetResponse { success: true };
        Ok(tonic::Response::new(reply))
    }

    async fn query(
        &self,
        request: tonic::Request<QueryRequest>,
    ) -> Result<tonic::Response<QueryResponse>, tonic::Status> {
        let keys = request.into_inner().key;
        let map = self.map.lock().unwrap(); // Acquire the lock

        let items: Vec<rs_datastore::nestedmap::Item> = map.query(&keys, None); // TODO - support GetOptions

        if items.is_empty() {
            return Err(tonic::Status::not_found(
                "No items found for the given keys",
            ));
        }

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
    let addr = "127.0.0.1:7777".parse()?;
    let my_datastore = MyDatastore::new(3);

    Server::builder()
        .add_service(DatastoreServer::new(my_datastore))
        .serve(addr)
        .await?;

    Ok(())
}
