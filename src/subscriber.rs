use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use anyhow::Result;
use crate::api::{is_smaller_redis_id, Api};
use crate::storage::Storage;
use tokio::sync::RwLock;

pub type SubHandler = Box<dyn Fn(String, Vec<Vec<u8>>) + Send + Sync + 'static>;

#[derive(Clone)]
pub struct Subscriber {
    client: Arc<RwLock<Api>>,
    subs: Arc<Mutex<HashMap<String, Subscription>>>,
}

struct Subscription {
    handlers: Vec<(u32, SubHandler)>,
    id: String,
    next_id: Option<String>,
}

impl  Subscriber {
    pub async fn new(client: Arc<RwLock<Api>>) -> Self {
        let subs = Arc::new(Mutex::new(HashMap::new()));
        let subscriber = Self {
            client: client.clone(),
            subs: subs.clone(),
        };

        tokio::spawn(async move {
            loop {
                if let Err(e) = Self::poll_messages(client.clone(), &subs).await {
                    eprintln!("Poll error: {}", e);
                    break;
                }
            }
        });

        subscriber
    }

    async fn poll_messages(client: Arc<RwLock<Api>>, subs: &Arc<Mutex<HashMap<String, Subscription>>>) -> Result<()> {
        let stream_ids = subs.lock().unwrap()
            .iter()
            .map(|(stream, sub)| (stream.clone(), sub.id.clone()))
            .collect();

        let messages = client.read().await.get_messages(stream_ids).await?;
        
        for msg in messages {
            let mut subs = subs.lock().unwrap();
            if let Some(sub) = subs.get_mut(&msg.stream) {
                sub.id = msg.last_id.clone();
                
                if let Some(next_id) = &sub.next_id {
                    sub.id = next_id.clone();
                    sub.next_id = None;
                }

                for (_, handler) in &sub.handlers {
                    handler(msg.stream.to_owned(), msg.messages.clone());
                }
            }
        }
        
        Ok(())
    }

    pub fn subscribe(&self, stream: &str, handler: SubHandler) -> SubscriptionTicket {
        let mut subs = self.subs.lock().unwrap();
        let sub = subs.entry(stream.to_string())
            .or_insert_with(|| 
                Subscription {
                    handlers: Vec::new(),
                    id: "0".to_string(),
                    next_id: None,
                }
            );
        // Generate unique handler ID using atomic counter
        static HANDLER_ID: AtomicU32 = AtomicU32::new(1);
        let handler_id = HANDLER_ID.fetch_add(1, Ordering::Relaxed);
        sub.handlers.push((handler_id, handler));

        SubscriptionTicket {
            handler_id,
            redis_id: sub.id.clone(),
            stream: stream.to_string(),
        }
    }

    pub fn unsubscribe(&self, stream: &str, handler_id: u32) {
        let mut subs = self.subs.lock().unwrap();
        if let Some(sub) = subs.get_mut(stream) {
            sub.handlers.retain(|(id, _)| *id != handler_id);
            if sub.handlers.is_empty() {
                subs.remove(stream);
            }
        }
    }

    pub fn ensure_sub_id(&self, stream: &str, id: &str) {
        let mut subs = self.subs.lock().unwrap();
        if let Some(sub) = subs.get_mut(stream) {
            if is_smaller_redis_id(id, &sub.id) {
                sub.next_id = Some(id.to_string());
            }
        }
    }
}

#[derive(Clone)]
pub struct SubscriptionTicket {
    pub handler_id:u32,
    pub stream: String,
    pub redis_id: String,
}
