use std::collections::BinaryHeap;
use std::sync::atomic::Ordering;
use std::sync::{atomic::AtomicI64, Arc};
use std::time::SystemTime;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::sync::oneshot;

use crate::nestedmap::options::{GetOptions, SetOptions};
use crate::nestedmap::NestedMap;
use event::Event;
use expiration::ExpirationEntry;

pub use crate::nestedmap::Item;

pub mod event;
pub mod expiration;

#[derive(Debug)]
pub struct Datastore {
    id_counter: Arc<AtomicI64>,
    event_sender: mpsc::Sender<Event>,
}

impl Datastore {
    pub fn new(_max_history: usize) -> Self {
        //env_logger::init();

        let (sender, receiver) = mpsc::channel::<Event>(10000);

        let datastore = Datastore {
            id_counter: Arc::new(AtomicI64::new(0)),
            event_sender: sender,
        };

        datastore.event_loop(receiver);
        datastore
    }

    // Async method to expose set functionality
    pub async fn set(&self, key: String, value: &[u8], options: Option<SetOptions>) {
        let sender = self.event_sender.clone();

        let id = self.id_counter.fetch_add(1, Ordering::Relaxed);

        if let Some(ref options) = options {
            if options.ttl.as_millis() > 0 {
                let expires_at = SystemTime::now() + options.ttl;

                let entry = ExpirationEntry {
                    id,
                    key: key.to_string(),
                    expires_at,
                };

                let sender = self.event_sender.clone();
                let _ = sender.send(Event::TTLInsert(entry)).await;
            }
        }

        let new_item = Item {
            key: key.to_string(),
            value: value.to_vec(),
            timestamp: SystemTime::now(),
            id,
        };

        let _ = sender.send(Event::Set(new_item, options)).await;
    }

    pub async fn get(&self, key: &str) -> Option<Item> {
        let sender = self.event_sender.clone();
        let (get_tx, get_rx) = oneshot::channel();

        let get_event = Event::Get(key.to_string(), get_tx);
        let _ = sender.send(get_event).await;

        // Await the response
        match get_rx.await {
            Ok(response) => return response,
            Err(_) => return None, // TODO: handle this
        }
    }

    pub async fn query(&self, key: &str, options: Option<GetOptions>) -> Vec<Item> {
        let sender = self.event_sender.clone();
        let (query_tx, query_rx) = oneshot::channel();

        let query_event = Event::Query(key.to_string(), options, query_tx);
        let _ = sender.send(query_event).await;

        // Await the response
        match query_rx.await {
            Ok(response) => return response,
            Err(_) => return vec![], // TODO: handle this
        }
    }
}

mod tests {
    use super::*;
    use std::time::Duration;

    use tokio::time::sleep;

    #[tokio::test]
    async fn test_expiration() {
        let ds = Arc::new(Datastore::new(1));

        // set value with ttl
        println!("#### SETTING A");
        ds.clone()
            .set(
                "a.b.c".to_string(),
                b"abc",
                Some(SetOptions::new().ttl(Duration::from_millis(100))),
            )
            .await;
        ds.clone()
            .set(
                "a.b.b".to_string(),
                b"abc",
                Some(SetOptions::new().ttl(Duration::from_millis(100))),
            )
            .await;

        println!("#### SETTING B");
        ds.clone()
            .set(
                "a.b.d".to_string(),
                b"abd",
                Some(SetOptions::new().ttl(Duration::from_millis(200))),
            )
            .await;

        println!("#### SETTING C");
        ds.clone()
            .set(
                "a.b.e".to_string(),
                b"abe",
                Some(SetOptions::new().ttl(Duration::from_millis(400))),
            )
            .await;

        // get values
        let items = ds.query("a.b.>", None).await;
        assert_eq!(items.len(), 4);

        // check first expiration
        sleep(Duration::from_millis(110)).await;
        let items = ds.query("a.b.>", None).await;
        assert_eq!(items.len(), 2);

        if ds.get("a.b.c").await.is_some() {
            panic!("Found key that should have been removed! a.b.c")
        }

        // check second expiration
        sleep(Duration::from_millis(110)).await;
        let items = ds.query("a.b.>", None).await;
        assert_eq!(items.len(), 1);

        if ds.get("a.b.d").await.is_some() {
            panic!("Found key that should have been removed! a.b.d")
        }

        // check last expiration
        sleep(Duration::from_millis(210)).await;
        let items = ds.query("a.b.>", None).await;
        assert_eq!(items.len(), 0);

        if ds.get("a.b.e").await.is_some() {
            panic!("Found key that should have been removed! a.b.e")
        }
    }
}
