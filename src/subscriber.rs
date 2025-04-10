use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use anyhow::Result;
use crate::api::{is_smaller_redis_id, Api};
use tokio::sync::RwLock;

pub type SubHandler = Box<dyn Fn(String, Vec<Vec<u8>>) + Send + Sync + 'static>;

#[derive(Clone)]
pub struct Subscriber {
    subscriptions: Arc<Mutex<HashMap<String, Subscription>>>,
}

struct Subscription {
    handlers: Vec<(usize, SubHandler)>,
    id: String,
    next_id: Option<String>,
}

impl Subscriber {
    pub async fn new(client: Arc<RwLock<Api>>) -> Self {
        let subscriptions = Arc::new(Mutex::new(HashMap::new()));
        let subscriber = Self {
            subscriptions: subscriptions.clone(),
        };

        tokio::spawn(async move {
            loop {
                if let Err(e) = Self::poll_messages(client.clone(), &subscriptions).await {
                    eprintln!("Poll error: {}", e);
                    break;
                }
            }
        });

        subscriber
    }

    async fn poll_messages(client: Arc<RwLock<Api>>, subscriptions: &Arc<Mutex<HashMap<String, Subscription>>>) -> Result<()> {
        let stream_ids = subscriptions.lock().unwrap()
            .iter()
            .map(|(stream, subscription)| (stream.clone(), subscription.id.clone()))
            .collect();
        let messages = client.read().await.get_messages(stream_ids).await?;
        
        for msg in messages {
            let mut subscriptions = subscriptions.lock().unwrap();
            if let Some(subscription) = subscriptions.get_mut(&msg.stream) {
                subscription.id = msg.last_id.clone();
                
                if let Some(next_id) = &subscription.next_id {
                    subscription.id = next_id.clone();
                    subscription.next_id = None;
                }

                for (_, handler) in &subscription.handlers {
                    handler(msg.stream.to_owned(), msg.messages.clone());
                }
            }
        }
        
        Ok(())
    }

    pub fn subscribe(&self, stream: &str, handler: SubHandler) -> SubscriptionTicket {
        let mut subscriptions = self.subscriptions.lock().unwrap();
        let subscription = subscriptions.entry(stream.to_string())
            .or_insert_with(|| 
                Subscription {
                    handlers: Vec::new(),
                    id: "0".to_string(),
                    next_id: None,
                }
            );

        let mut rng = fastrand::Rng::new();
        let id = rng.usize(0..usize::MAX);
        let handler_id = id;

        subscription.handlers.push((handler_id, handler));

        SubscriptionTicket {
            handler_id,
            redis_id: subscription.id.clone(),
            stream: stream.to_string(),
        }
    }

    pub fn unsubscribe(&self, stream: &str, handler_id: usize) {
        let mut subscriptions = self.subscriptions.lock().unwrap();
        if let Some(subscription) = subscriptions.get_mut(stream) {
            subscription.handlers.retain(|(id, _)| *id != handler_id);
            if subscription.handlers.is_empty() {
                subscriptions.remove(stream);
            }
        }
    }

    pub fn ensure_sub_id(&self, stream: &str, id: &str) {
        let mut subscriptions = self.subscriptions.lock().unwrap();
        if let Some(subscription) = subscriptions.get_mut(stream) {
            if is_smaller_redis_id(id, &subscription.id) {
                subscription.next_id = Some(id.to_string());
            }
        }
    }
}

#[derive(Clone)]
pub struct SubscriptionTicket {
    pub handler_id:usize,
    pub stream: String,
    pub redis_id: String,
}