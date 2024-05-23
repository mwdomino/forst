use std::time::Duration;
use std::time::SystemTime;

use super::Datastore;
use super::ExpirationEntry;
use tokio::sync::mpsc::Receiver;
use tokio::time;
use tokio::time::Sleep;

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

        println!("Starting event loop");

        tokio::spawn(async move {
            let timer = tokio::time::sleep(Duration::from_secs(u64::MAX));
            tokio::pin!(timer);

            loop {
                tokio::select! {
                    event = receiver.recv() => {
                        println!("Received event: {:?}", event);

                        if let Some(event) = event {
                            match event {
                                Event::TTLInsert(entry) => {
                                    let mut ttl_guard = ttl.lock().await;
                                    println!("Inserted entry: key:{} id:{}", entry.key, entry.id);

                                    if let Some(next_expiry) = ttl_guard.peek() {
                                        println!("there was an entry in ttl already");
                                        if next_expiry.expires_at > entry.expires_at {
                                            let now = SystemTime::now();
                                            let duration = entry.expires_at.duration_since(now).unwrap_or(Duration::new(0, 0));

                                            timer.as_mut().reset(time::Instant::now() + duration);
                                            println!("Old timer updated using key:{} id:{}!", entry.key, entry.id);
                                        }
                                    } else {
                                        println!("No ttl entry found, inserting new");
                                        let now = SystemTime::now();
                                        let duration = entry.expires_at.duration_since(now).unwrap_or(Duration::new(0, 0));

                                        timer.as_mut().reset(time::Instant::now() + duration);
                                    }

                                    ttl_guard.push(entry.clone());
                                },
                                Event::TTLExpired(entry) => {
                                    let mut map_guard = map.lock().await;
                                    map_guard.delete_by_id(&entry.key, entry.id);

                                    println!("Deleted entry: key:{} id:{}", entry.key, entry.id);

                                    // need to update the timer now
                                    let ttl_guard = ttl.lock().await;
                                    if let Some(next_expiry) = ttl_guard.peek() {
                                        let now = SystemTime::now();
                                        let duration = next_expiry.expires_at.duration_since(now).unwrap_or(Duration::new(0, 0));
                                        timer.as_mut().reset(time::Instant::now() + duration);
                                    } else {
                                        // just set a long timer as the default
                                        timer.as_mut().reset(time::Instant::now() + Duration::from_secs(5000));
                                    }
                                },
                                Event::Notify => {
                                    println!("Notify event received");
                                }
                            }
                        } else {
                            break;
                        }
                    },
                    () = &mut timer => {
                    //_ = timer.as_mut().unwrap(), if timer.is_some() => {
                        // Timer expired, process expiration
                        let mut ttl_guard = ttl.lock().await;
                        if let Some(min_entry) = ttl_guard.pop() {
                            println!("Timer expired for key:{} id:{}", min_entry.key, min_entry.id);

                            let _ = sender.send(Event::TTLExpired(min_entry)).await;
                        }
                    }
                }
            }
        });
    }
}
