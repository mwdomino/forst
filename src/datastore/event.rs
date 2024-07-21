use log::{info};

use std::time::Duration;
use std::time::SystemTime;

use super::Datastore;
use super::ExpirationEntry;
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
    Notify,
}

impl Datastore {
    pub fn event_loop(&self, mut receiver: Receiver<Event>) {
        let map = self.map.clone();
        let ttl = self.ttl.clone();
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
                                Event::TTLInsert(entry) => {
                                    let mut ttl_guard = ttl.lock().await;
                                    info!("Inserted entry: key:{} id:{}", entry.key, entry.id);
                                    let now = SystemTime::now();

                                    if let Some(next_expiry) = ttl_guard.peek() {
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

                                    ttl_guard.push(entry.clone());
                                },
                                Event::TTLExpired(entry) => {
                                    let mut map_guard = map.lock().await;
                                    map_guard.delete_by_id(&entry.key, entry.id);

                                    info!("Deleted entry: key:{} id:{}", entry.key, entry.id);

                                    // need to update the timer now
                                    let ttl_guard = ttl.lock().await;
                                    if let Some(next_expiry) = ttl_guard.peek() {
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
                        let mut ttl_guard = ttl.lock().await;

                        if let Some(next_expiry) = ttl_guard.peek() {
                            // ensure entry is expired
                            if next_expiry.expires_at < SystemTime::now() {
                                if let Some(min_entry) = ttl_guard.pop() {
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
