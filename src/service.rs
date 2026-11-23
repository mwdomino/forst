use std::future::Future;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::server::TcpIncoming;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use crate::datastore::{self, Change, Datastore, EventKind, SetItem};
use crate::nestedmap::options::GetOptions;
use proto::datastore_server::{Datastore as DatastoreTrait, DatastoreServer};
use proto::*;

pub mod proto {
    tonic::include_proto!("datastore");
}

// Largest accepted request, so a big SetBatch is not refused as a permanent error.
const MAX_MESSAGE_BYTES: usize = 64 << 20;
// Events per message while streaming a subscription's initial state.
const SNAPSHOT_CHUNK: usize = 1024;

#[derive(Debug)]
pub struct Service {
    datastore: Arc<Datastore>,
}

impl Service {
    pub fn new(datastore: Arc<Datastore>) -> Self {
        Service { datastore }
    }
}

// Serves the datastore on `listener` until `shutdown` completes.
pub async fn serve(
    listener: TcpListener,
    datastore: Arc<Datastore>,
    shutdown: impl Future<Output = ()>,
) -> Result<(), tonic::transport::Error> {
    let service =
        DatastoreServer::new(Service::new(datastore)).max_decoding_message_size(MAX_MESSAGE_BYTES);
    Server::builder()
        .add_service(service)
        .serve_with_incoming_shutdown(
            TcpIncoming::from(listener).with_nodelay(Some(true)),
            shutdown,
        )
        .await
}

fn to_proto(item: datastore::Item) -> Item {
    Item {
        key: item.key,
        value: item.value,
        timestamp_ns: item.timestamp,
    }
}

fn to_set_item(req: SetRequest) -> SetItem {
    let options = req.options.unwrap_or_default();
    SetItem {
        key: req.key,
        value: req.value,
        timestamp_ns: req.timestamp_ns,
        ttl_ns: options.ttl.saturating_mul(1_000_000_000),
        preserve_history: options.preserve_history,
    }
}

fn event_to_proto(e: &datastore::Event) -> Event {
    let kind = match e.kind {
        EventKind::Set => event::Kind::Set,
        EventKind::Delete => event::Kind::Delete,
        EventKind::Expire => event::Kind::Expire,
    };
    Event {
        kind: kind.into(),
        item: Some(to_proto(e.item.clone())),
    }
}

#[tonic::async_trait]
impl DatastoreTrait for Service {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let item = self.datastore.get(&request.into_inner().key).map(to_proto);
        Ok(Response::new(GetResponse { item }))
    }

    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        let rejected = self
            .datastore
            .set_batch(vec![to_set_item(request.into_inner())]);
        match rejected.first() {
            Some(r) => Err(Status::invalid_argument(r.reason)),
            None => Ok(Response::new(SetResponse { success: true })),
        }
    }

    async fn set_batch(
        &self,
        request: Request<SetBatchRequest>,
    ) -> Result<Response<SetBatchResponse>, Status> {
        let items = request
            .into_inner()
            .items
            .into_iter()
            .map(to_set_item)
            .collect();
        let rejected = self
            .datastore
            .set_batch(items)
            .into_iter()
            .map(|r| Rejected {
                index: r.index as u32,
                reason: r.reason.to_string(),
            })
            .collect();
        Ok(Response::new(SetBatchResponse { rejected }))
    }

    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<QueryResponse>, Status> {
        let inner = request.into_inner();
        let options = inner
            .options
            .and_then(|opts| opts.history_count)
            .map(|count| GetOptions {
                history_count: count.max(0) as usize,
            });

        let items = self.datastore.query(&inner.key, options);
        Ok(Response::new(QueryResponse {
            items: items.into_iter().map(to_proto).collect(),
        }))
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let success = self.datastore.delete(&request.into_inner().key);
        Ok(Response::new(DeleteResponse { success }))
    }

    async fn delete_at_index(
        &self,
        request: Request<DeleteAtIndexRequest>,
    ) -> Result<Response<DeleteAtIndexResponse>, Status> {
        let req = request.into_inner();
        let index = usize::try_from(req.index)
            .map_err(|_| Status::invalid_argument("index must not be negative"))?;
        let success = self.datastore.delete_at_index(&req.key, index);
        Ok(Response::new(DeleteAtIndexResponse { success }))
    }

    type SubscribeStream = ReceiverStream<Result<SubscribeResponse, Status>>;

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let pattern = request.into_inner().pattern;
        let mut sub = self.datastore.subscribe(&pattern);
        // Small buffers: a slow client fills them, then lags on a shard and is cut off
        let mut changes = sub.changes(&pattern, 16);
        let (tx, rx) = mpsc::channel(16);

        tokio::spawn(async move {
            let mut snapshot = sub.snapshot.into_iter().map(|item| Event {
                kind: event::Kind::Set.into(),
                item: Some(to_proto(item)),
            });
            loop {
                let events: Vec<Event> = snapshot.by_ref().take(SNAPSHOT_CHUNK).collect();
                if events.is_empty() {
                    break;
                }
                let msg = SubscribeResponse {
                    lsn: 0,
                    events,
                    sync: false,
                    shard: 0,
                };
                if tx.send(Ok(msg)).await.is_err() {
                    return;
                }
            }
            let sync = SubscribeResponse {
                lsn: 0,
                events: Vec::new(),
                sync: true,
                shard: 0,
            };
            if tx.send(Ok(sync)).await.is_err() {
                return;
            }

            loop {
                let msg = match changes.recv().await {
                    Some(Change::Events { shard, lsn, events }) => Ok(SubscribeResponse {
                        lsn,
                        events: events.iter().map(event_to_proto).collect(),
                        sync: false,
                        shard,
                    }),
                    Some(Change::Lagged) => Err(Status::aborted(
                        "subscriber fell too far behind; subscribe again",
                    )),
                    None => return,
                };
                let lagged = msg.is_err();
                if tx.send(msg).await.is_err() || lagged {
                    return;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
