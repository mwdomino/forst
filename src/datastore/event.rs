use log::{info};

use std::collections::BinaryHeap;
use std::time::Duration;
use std::time::SystemTime;

use crate::nestedmap::NestedMap;

use super::Datastore;
use super::ExpirationEntry;
use super::Item;
use tokio::sync::mpsc::Receiver;
use tokio::time;
use tokio::time::Sleep;

pub struct Timer {
    timer: Option<std::pin::Pin<Box<Sleep>>>,
}

impl Timer {
    pub fn new() -> Self {
        Self {
            timer: None,
        }
    }

    pub fn reset(&mut self, duration: Duration) {
        self.timer = Some(Box::pin(tokio::time::sleep(duration)));
    }

    pub fn disable(&mut self) {
        self.timer = None;
    }

    pub fn is_active(&self) -> bool {
        self.timer.is_some()
    }

    pub fn wait(&mut self) -> Option<impl std::future::Future<Output = ()> + '_> {
        self.timer.as_mut().map(|timer| timer.as_mut())
    }
}

#[derive(Debug)]
pub enum Event {
    TTLInsert(ExpirationEntry),
    TTLExpired(ExpirationEntry),
    Set(Item),
    Notify,
}

impl Datastore {
    pub fn event_loop(&self, mut receiver: Receiver<Event>) {
        let mut map = NestedMap::new(5);
        let mut ttl: BinaryHeap<ExpirationEntry> = BinaryHeap::new();
        let sender = self.event_sender.clone();

        info!("Starting event loop");

        tokio::spawn(async move {
            let mut timer = Timer::new();

            loop {
                tokio::select! {
                    event = receiver.recv() => {
                        info!("Received event: {:?}", event);

                        if let Some(event) = event {
                            match event {
                                Event::Set(item) => {
                                    map.set(&item.key, &item, None);
                                }
                                Event::TTLInsert(entry) => {
                                    info!("Inserted entry: key:{} id:{}", entry.key, entry.id);
                                    let now = SystemTime::now();

                                    if let Some(next_expiry) = ttl.peek() {
                                        info!("there was an entry in ttl already");
                                        if next_expiry.expires_at > entry.expires_at {
                                            let duration = entry.expires_at.duration_since(now).unwrap_or(Duration::new(0, 0));
                                            timer.reset(duration);
                                            info!("Old timer updated using key:{} id:{}!", entry.key, entry.id);
                                        }
                                    } else {
                                        info!("No ttl entry found, inserting new");
                                        let duration = entry.expires_at.duration_since(now).unwrap_or(Duration::new(0, 0));
                                        timer.reset(duration);
                                    }

                                    ttl.push(entry.clone());
                                },
                                Event::TTLExpired(entry) => {
                                    map.delete_by_id(&entry.key, entry.id);

                                    info!("Deleted entry: key:{} id:{}", entry.key, entry.id);

                                    // need to update the timer now
                                    if let Some(next_expiry) = ttl.peek() {
                                        let now = SystemTime::now();
                                        let duration = next_expiry.expires_at.duration_since(now).unwrap_or(Duration::new(0, 0));
                                        timer.reset(duration);
                                    } else {
                                        // just set a long timer as the default
                                        timer.reset(Duration::from_secs(5000));
                                    }
                                },
                                Event::Notify => {
                                    info!("Notify event received");
                                }
                            }
                        } else {
                            break;
                        }
                    },
                    _ = async {
                        if let Some(future) = timer.wait() {
                            future.await;
                        } else {
                            futures::future::pending::<()>().await;
                        }
                    } => {
                        // Timer expired, process expiration
                        if let Some(next_expiry) = ttl.peek() {
                            // ensure entry is expired
                            if next_expiry.expires_at < SystemTime::now() {
                                if let Some(min_entry) = ttl.pop() {
                                    info!("Timer expired for key:{} id:{}", min_entry.key, min_entry.id);
                                    timer.disable();
                                    let _ = sender.send(Event::TTLExpired(min_entry)).await;
                                }
                            }
                        }
                    }
                }
            }
        });
    }
}
