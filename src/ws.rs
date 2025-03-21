use std::collections::HashSet;
use std::sync::Arc;
use anyhow::Result;
use axum::extract::ws::WebSocket;
use futures_util::{SinkExt, StreamExt};
use axum::extract::ws::Message;
use tracing::{info, error};
use yrs::encoding::read::{Cursor, Read};
use yrs::encoding::write::Write;
use yrs::updates::encoder::{Encode, Encoder, EncoderV1};
use yrs::{ReadTxn, StateVector, Transact};
use tokio::sync::RwLock;
use tokio::sync::Mutex;
use crate::api::{Api, compute_redis_room_stream_name, is_smaller_redis_id};
use crate::protocol::{
    MSG_SYNC, MSG_AWARENESS, 
    MSG_SYNC_STEP1, MSG_SYNC_STEP2, MSG_SYNC_UPDATE,
    encode_sync_step1, encode_sync_step2, encode_awareness_update,
    encode_awareness_user_disconnected
};
use crate::subscriber::{Subscriber, SubHandler};
use crate::storage::Storage;


pub async fn make_websocket_server(
    store: Arc<Box<dyn Storage>>,
    redis_prefix: &str
) -> Result<YWebsocketServer> {
    let client = Arc::new(RwLock::new(Api::new(store, redis_prefix.to_string())?));

    let subscriber = Arc::new(Subscriber::new(client.clone()).await);

    Ok(YWebsocketServer::new(client, subscriber))
}

pub struct User {
    id: usize,
    room: String,
    has_write_access: bool,
    userid: String,
    initial_redis_sub_id: String,
    subs: HashSet<String>,
    awareness_id: Option<u64>,
    awareness_last_clock: u64,
}

impl User {
    pub fn new(room: String, has_write_access: bool, userid: String) -> Self {
        static mut ID_COUNTER: usize = 0;
        let id = unsafe {
            ID_COUNTER += 1;
            ID_COUNTER
        };
        
        Self {
            id,
            room,
            has_write_access,
            userid,
            initial_redis_sub_id: "0".to_string(),
            subs: HashSet::new(),
            awareness_id: None,
            awareness_last_clock: 0,
        }
    }
}

pub struct YWebsocketServer {
    client: Arc<RwLock<Api>>,
    subscriber: Arc<Subscriber>,
}

impl  YWebsocketServer {
    pub fn new(client: Arc<RwLock<Api>>, subscriber: Arc<Subscriber>) -> Self {
        Self {
            client,
            subscriber,
        }
    }
   
    pub async fn handle(
        &self, 
        user: Arc<RwLock<User>>,
        websocket: WebSocket,
        init_doc_callback: impl Fn(&str, &str, &Api) -> Result<()> + Send + Sync + 'static
    ) -> Result<()> {
        let client = self.client.clone();
        let subscriber = self.subscriber.clone();
        let init_doc_callback = Arc::new(init_doc_callback);
        
        if let Err(e) = handle_connection(
            user,
            websocket, 
            client, 
            subscriber,
            init_doc_callback
        ).await {
            error!("Connection error: {}", e);
        }
        
        Ok(())
    }
}

async fn handle_connection(
    user: Arc<RwLock<User>>,
    websocket: WebSocket,
    client: Arc<RwLock<Api>>,
    subscriber: Arc<Subscriber>,
    init_doc_callback: Arc<impl Fn(&str, &str, &Api) -> Result<()> + Send + Sync>
) -> Result<()> {
    let (mut ws_sender, mut ws_receiver) = websocket.split();
    
    // Set up Redis subscription
    let room = user.read().await.room.clone();

    let stream_name = compute_redis_room_stream_name(room.as_str(), "index", &client.read().await.prefix);
    
    let  ws_sender= Arc::new(Mutex::new(ws_sender));
    let ws_sender_clone = ws_sender.clone();
    // Create a handler for Redis messages
    let redis_message_handler: SubHandler = Box::new(move |_stream, messages| {
        let ws_sender = ws_sender_clone.clone();
        
        tokio::spawn(async move {
            let mut sender = ws_sender.lock().await;
            if messages.len() == 1 {
                if let Err(e) = sender.send(Message::Binary(messages[0].clone().into())).await {
                    error!("Failed to send message: {}", e);
                }
            } else {
                let mut encoder = EncoderV1::new();
                for msg in &messages {
                    encoder.write_buf(msg);
                }

                if let Err(e) = sender.send(Message::Binary(encoder.to_vec().into())).await {
                    error!("Failed to send combined message: {}", e);
                }
            }
        });
    });
    
    // Subscribe to Redis updates
    let mut user_guard = user.write().await;
    user_guard.subs.insert(stream_name.clone());
    let subscription_ticket = subscriber.subscribe(stream_name.as_str(), redis_message_handler);
    user_guard.initial_redis_sub_id = subscription_ticket.redis_id.clone();
    drop(user_guard);
    
    // Get the initial document state
    let mut client_guard = client.write().await;
    let index_doc = client_guard.get_doc(&room, "index").await?;
    
    //TODO: Initialize document if needed
    // if &index_doc.ydoc.transact().store().clients().is_empty() {
    //     init_doc_callback(&room, "index", &client)?;
    // }
    
    // Send initial sync messages
    let sv = index_doc.ydoc.transact_mut().state_vector().encode_v1();

    let mut ws_sender = ws_sender.lock().await;

    ws_sender.send(Message::Binary(encode_sync_step1(&sv).into())).await?;
    
    let update = index_doc.ydoc.transact_mut().encode_state_as_update_v1(&StateVector::default());
    ws_sender.send(Message::Binary(encode_sync_step2(&update).into())).await?;
    
    // Send awareness states if any
    if index_doc.awareness.iter().collect::<Vec<_>>().len() > 0 {
        let client_ids: Vec<u64> = index_doc.awareness.iter().map(|(id, _)| id).collect();

        let awareness_update = encode_awareness_update(&index_doc.awareness, client_ids)?;
        ws_sender.send(Message::Binary(awareness_update.into())).await?;
    }
    
    // Check if we need to update subscription ID
    let user_guard = user.read().await;
    if is_smaller_redis_id(&index_doc.redis_last_id, &user_guard.initial_redis_sub_id) {
        subscriber.ensure_sub_id(&stream_name, &index_doc.redis_last_id);
    }
    drop(user_guard);
    
    // Message handling loop
    while let Some(msg) = ws_receiver.next().await {
        match msg {
            Ok(Message::Binary(data)) => {
                let mut user_guard = user.write().await;
                
                // Skip messages from users without write access
                if !user_guard.has_write_access {
                    continue;
                }
                
                // Process message based on type
                if data.len() > 0 {
                    if (data[0] == MSG_SYNC && (data.len() > 1 && (data[1] == MSG_SYNC_UPDATE || data[1] == MSG_SYNC_STEP2))) ||
                       data[0] == MSG_AWARENESS {
                        
                        // Handle awareness updates
                        if data[0] == MSG_AWARENESS {
                            let mut decoder = yrs::updates::decoder::DecoderV1::new(Cursor::new(data.iter().as_slice()));
                            let _ = decoder.read_u64()?; // read message type
                            let _ = decoder.read_u64()?; // read length of awareness update
                            let alen: u64 = decoder.read_u64()?; // number of awareness updates
                            let aw_id = decoder.read_u64()?;
                            
                            // Only update awareness if len=1 and either no previous ID or same ID
                            if alen == 1 && (user_guard.awareness_id.is_none() || user_guard.awareness_id == Some(aw_id)) {
                                user_guard.awareness_id = Some(aw_id);
                                user_guard.awareness_last_clock = decoder.read_u64().unwrap_or(0);
                            }
                        }
                        
                        // Forward message to Redis
                        client.write().await.add_message(&user_guard.room, "index", data.into()).await?;
                    } else if data[0] == MSG_SYNC && data.len() > 1 && data[1] == MSG_SYNC_STEP1 {
                        // can be safely ignored because we send the full initial state at the beginning
                    } else {
                        error!("Unexpected message type: {}", data[0]);
                    }
                }
                
                drop(user_guard);
            },
            Ok(Message::Close(_)) => {
                break;
            },
            Err(e) => {
                error!("WebSocket error: {}", e);
                break;
            },
            _ => {}
        }
    }
    
    // Handle disconnection
    let mut user_guard = user.read().await;
    
    // Send awareness disconnection message if needed
    if let Some(awareness_id) = user_guard.awareness_id {
        client.write().await.add_message(
            &user_guard.room, 
            "index", 
            encode_awareness_user_disconnected(awareness_id, user_guard.awareness_last_clock)?
        ).await?;
    }
    
    // Clean up subscriptions
    for topic in &user_guard.subs {
        subscriber.unsubscribe(topic, subscription_ticket.handler_id);
    }
    
    info!("Client disconnected: user_id={}", user_guard.id);
    
    Ok(())
}