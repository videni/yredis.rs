use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use anyhow::Result;
use rand::Rng;
use crate::api::{is_smaller_redis_id, Api};

type SubHandler = Box<dyn Fn(String, Vec<Vec<u8>>) + Send + Sync + 'static>;

#[derive(Clone)]
pub struct Subscriber {
    client: Arc<Api>,
    subs: Arc<Mutex<HashMap<String, Subscription>>>,
}

struct Subscription {
    handlers: Vec<SubHandler>,
    id: String,
    next_id: Option<String>,
}

impl Subscriber {
    pub async fn new(client: Api) -> Self {
        let subs = Arc::new(Mutex::new(HashMap::new()));
        let shared_client = Arc::new(client);
        let subscriber = Self {
            client: shared_client.clone(),
            subs: subs.clone(),
        };

        tokio::spawn(async move {
            loop {
                if let Err(e) = Self::poll_messages(shared_client.clone(), &subs).await {
                    eprintln!("Poll error: {}", e);
                    break;
                }
            }
        });

        subscriber
    }

    async fn poll_messages(client: Arc<Api>, subs: &Arc<Mutex<HashMap<String, Subscription>>>) -> Result<()> {
        let stream_ids = subs.lock().unwrap()
            .iter()
            .map(|(stream, sub)| (stream.clone(), sub.id.clone()))
            .collect();

        let messages = client.get_messages(stream_ids).await?;
        
        for msg in messages {
            let mut subs = subs.lock().unwrap();
            if let Some(sub) = subs.get_mut(&msg.stream) {
                sub.id = msg.last_id.clone();
                
                if let Some(next_id) = &sub.next_id {
                    sub.id = next_id.clone();
                    sub.next_id = None;
                }

                for handler in &sub.handlers {
                    handler(msg.stream.to_owned(), msg.messages.clone())
                }
            }
        }
        
        Ok(())
    }

    pub fn subscribe(&self, stream: &str, handler: SubHandler) {
        let mut subs = self.subs.lock().unwrap();
        let sub = subs.entry(stream.to_string())
            .or_insert_with(||
                Subscription {
                    handlers: Vec::new(),
                    id: "0".to_string(),
                    next_id: None,
                }
            );
        //TODO: 每个订阅的可能有自己的 id, next_id, 而不是。
        sub.handlers.push(handler);
    }

    pub fn unsubscribe(&self, stream: &str, handler_id: u32) {
        let mut subs = self.subs.lock().unwrap();
        if let Some(sub) = subs.get_mut(stream) {
           //TODO:  
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

// #[derive(Clone)]
// pub struct HandlerTicket {
//     handler_id:u32,
//     stream: String,
//     subscriber: Subscriber,
//     _handler: Arc<SubHandler>,
// }

// impl Drop for HandlerTicket {
//     fn drop(&mut self) {
//         self.subscriber.unsubscribe(&self.stream, self.handler_id);
//     }
// }