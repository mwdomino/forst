// End-to-end tests over real TCP against the gRPC service.

use std::collections::{BTreeSet, HashMap};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rs_datastore::datastore::Datastore;
use rs_datastore::service::proto::datastore_client::DatastoreClient;
use rs_datastore::service::proto::{event, *};
use rs_datastore::service::serve;
use tokio::net::{TcpListener, TcpStream};
use tonic::transport::Channel;
use tonic::Code;

const MAX_HISTORY: usize = 100;

async fn start_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    // Keys in these tests are "<prefix>.<device>.<metric>": shard by the first two segments
    let datastore = Arc::new(Datastore::with_shards(MAX_HISTORY, 4, 2));
    tokio::spawn(serve(listener, datastore, std::future::pending()));
    addr
}

// Forwards TCP to `upstream` and cuts every connection after a few milliseconds,
// so requests fail at random points: before, during, or after the server applies them.
async fn start_chaos_proxy(upstream: SocketAddr) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let mut n = 0u64;
        loop {
            n += 1;
            let (mut client, _) = listener.accept().await.unwrap();
            let lifetime = Duration::from_millis(5 + n * 7 % 60);
            tokio::spawn(async move {
                let mut server = TcpStream::connect(upstream).await.unwrap();
                let _ = tokio::time::timeout(
                    lifetime,
                    tokio::io::copy_bidirectional(&mut client, &mut server),
                )
                .await;
            });
        }
    });
    addr
}

async fn client(addr: SocketAddr) -> DatastoreClient<Channel> {
    let channel = Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .timeout(Duration::from_secs(2))
        .connect_lazy();
    DatastoreClient::new(channel)
}

fn set(key: String, ts: i64, ttl: i64) -> SetRequest {
    SetRequest {
        value: format!("{key}@{ts}").into_bytes(),
        key,
        timestamp_ns: ts,
        options: Some(SetOptions {
            preserve_history: true,
            ttl,
        }),
    }
}

// Like a collector: retry the same batch until it is acked.
async fn send_until_acked(
    client: &mut DatastoreClient<Channel>,
    batch: &SetBatchRequest,
    retries: &AtomicU64,
) {
    loop {
        match client.set_batch(batch.clone()).await {
            Ok(resp) => {
                assert!(resp.into_inner().rejected.is_empty());
                return;
            }
            Err(_) => {
                retries.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retried_batches_through_dropped_connections_lose_and_duplicate_nothing() {
    let server = start_server().await;
    let proxy = start_chaos_proxy(server).await;
    let retries = Arc::new(AtomicU64::new(0));
    let (collectors, batches, batch_size, keys) = (8, 20, 200, 1000);

    let mut tasks = Vec::new();
    for c in 0..collectors {
        let retries = retries.clone();
        tasks.push(tokio::spawn(async move {
            let mut client = client(proxy).await;
            let mut sent: Vec<(String, i64)> = Vec::new();
            for b in 0..batches {
                let items: Vec<SetRequest> = (0..batch_size)
                    .map(|i| {
                        // Unique timestamp per item across all collectors
                        let ts = 1_000_000_000_000 + ((c * batches + b) * batch_size + i) as i64;
                        set(format!("dev.{}.ctr", ts % keys), ts, 0)
                    })
                    .collect();
                sent.extend(items.iter().map(|i| (i.key.clone(), i.timestamp_ns)));
                let batch = SetBatchRequest { items };
                send_until_acked(&mut client, &batch, &retries).await;
                // Replay every batch once: an acked batch applied twice must change nothing
                send_until_acked(&mut client, &batch, &retries).await;
            }
            sent
        }));
    }

    let mut expected: HashMap<String, BTreeSet<i64>> = HashMap::new();
    for task in tasks {
        for (key, ts) in task.await.unwrap() {
            expected.entry(key).or_default().insert(ts);
        }
    }
    assert!(
        retries.load(Ordering::Relaxed) > 0,
        "the proxy never cut a request"
    );
    assert!(expected.values().all(|ts| ts.len() <= MAX_HISTORY));

    let mut direct = client(server).await;
    for (key, timestamps) in &expected {
        let items = direct
            .query(QueryRequest {
                key: key.clone(),
                options: Some(GetOptions {
                    history_count: Some(MAX_HISTORY as i64),
                }),
            })
            .await
            .unwrap()
            .into_inner()
            .items;
        let stored: Vec<i64> = items.iter().map(|i| i.timestamp_ns).collect();
        let want: Vec<i64> = timestamps.iter().rev().copied().collect();
        assert_eq!(stored, want, "key {key}");
    }
    println!("retries: {}", retries.load(Ordering::Relaxed));
}

#[tokio::test]
async fn subscribe_streams_state_then_set_delete_and_expire() {
    let server = start_server().await;
    let mut c = client(server).await;
    let batch = |items| SetBatchRequest { items };
    let now = || {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64
    };

    c.set_batch(batch(vec![set("dev.r1.cpu".into(), now(), 0)]))
        .await
        .unwrap();
    let mut stream = c
        .subscribe(SubscribeRequest {
            pattern: "dev.*.cpu".into(),
        })
        .await
        .unwrap()
        .into_inner();

    let first = stream.message().await.unwrap().unwrap();
    assert_eq!(first.events[0].item.as_ref().unwrap().key, "dev.r1.cpu");
    assert!(stream.message().await.unwrap().unwrap().sync);

    c.set_batch(batch(vec![
        set("dev.r2.mem".into(), now(), 0),
        set("dev.r2.cpu".into(), now(), 1),
    ]))
    .await
    .unwrap();
    c.delete(DeleteRequest {
        key: "dev.r1".into(),
    })
    .await
    .unwrap();

    let mut seen = Vec::new();
    while seen.len() < 3 {
        let msg = tokio::time::timeout(Duration::from_secs(3), stream.message())
            .await
            .expect("no event within 3s")
            .unwrap()
            .unwrap();
        for e in msg.events {
            seen.push((e.kind(), e.item.unwrap().key));
        }
    }
    let k = |s: &str| s.to_string();
    assert_eq!(
        seen,
        vec![
            (event::Kind::Set, k("dev.r2.cpu")),
            (event::Kind::Delete, k("dev.r1.cpu")),
            (event::Kind::Expire, k("dev.r2.cpu")),
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_subscriber_is_aborted() {
    let server = start_server().await;
    let mut c = client(server).await;
    let mut stream = c
        .subscribe(SubscribeRequest {
            pattern: ">".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(stream.message().await.unwrap().unwrap().sync);

    // Write far more than the subscriber buffers can hold, without reading
    let value = vec![0u8; 1024];
    for b in 0..3000i64 {
        let items = (0..20)
            .map(|i| SetRequest {
                key: format!("k.{i}"),
                value: value.clone(),
                timestamp_ns: 1 + b * 20 + i,
                options: None,
            })
            .collect();
        c.set_batch(SetBatchRequest { items }).await.unwrap();
    }

    let code = loop {
        match stream.message().await {
            Ok(Some(_)) => continue,
            Ok(None) => panic!("stream ended without an error"),
            Err(status) => break status.code(),
        }
    };
    assert_eq!(code, Code::Aborted);
}
