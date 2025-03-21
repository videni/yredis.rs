
use std::{collections::HashMap, sync::atomic::{AtomicBool, Ordering}};
use redis::{streams::{StreamAutoClaimOptions, StreamAutoClaimReply, StreamReadOptions, StreamReadReply}, ConnectionLike, FromRedisValue};
use regex::Regex;
use urlencoding::decode;
use anyhow::{Result, anyhow};
use redis::{Client, Commands, Script};
use uuid::Uuid;
use std::sync::Arc;
use yrs::{encoding::read::{Cursor, Read}, sync::{Awareness, AwarenessUpdate}, updates::decoder::{Decode, Decoder, DecoderV1}, Doc as YDoc, ReadTxn, StateVector, Transact, Update};
use tokio::time::{sleep, Duration};
use std::env;
use crate::storage::{Reference, Storage};
use tracing::*;

pub struct Api{
    store: Arc<Box<dyn Storage>>,
    pub prefix: String,
    consumer_name: String,
    redis_task_debounce: u64,
    redis_min_message_lifetime: u64,
    redis_worker_stream_name: String,
    redis_worker_group_name: String,
    redis: Client,
}

pub async fn create_api_client(store: Arc<Box<dyn Storage>>, redis_prefix: String) -> Result<Api> {
    let mut api = Api::new(store, redis_prefix)?;
    if !api.redis.get_connection()?.check_connection() {
        return Err(anyhow!("Redis connection failed"));
    }
    
    // Create consumer group if not exists
    let script = format!(r#"return redis.call('XGROUP', 'CREATE', '{}', '{}', '0', 'MKSTREAM')"#, api.redis_worker_stream_name, api.redis_worker_group_name);
    let result = Script::new(script.as_str())
        .invoke::<()>(&mut api.redis);

    match result {
        Ok(_) => Ok(api),
        Err(e) if e.to_string().contains("Consumer Group name already exists") => Ok(api),
        Err(e) => Err(e.into())
    }
}

impl  Api where  {
    pub fn new(store: Arc<Box<dyn Storage>>, prefix: String) -> Result<Self> {
        let redis_url = env::var("REDIS_URL").unwrap_or("redis://127.0.0.1:6379".to_owned());
        let redis = Client::open(redis_url)?;
        
        Ok(Self {
            store,
            prefix: prefix.clone(),
            consumer_name: Uuid::new_v4().to_string(),
            redis_task_debounce: env::var("REDIS_TASK_DEBOUNCE")
                .unwrap_or_else(|_| "10000".to_string())
                .parse()
                .unwrap_or(10000),
            redis_min_message_lifetime: env::var("REDIS_MIN_MESSAGE_LIFETIME")
                .unwrap_or_else(|_| "60000".to_string())
                .parse()
                .unwrap_or(60000),
            redis_worker_stream_name: format!("{}:worker", prefix),
            redis_worker_group_name: format!("{}:worker", prefix),
            redis,
        })
    }

    pub async fn get_messages(&self, streams: Vec<(String, String)>) -> Result<Vec<StreamMessage>> {
        if streams.is_empty() {
            sleep(Duration::from_millis(50)).await;
            return Ok(vec![]);
        }

        let (keys, values): (Vec<_>, Vec<_>) = streams.iter().cloned().unzip();

        let mut conn = self.redis.get_connection()?;
        let reads: Vec<StreamReadReply>  = conn.xread_options(
            &keys,
            &values,
            &StreamReadOptions::default().block(1000).count(1000),
        )?;

        Ok(reads.into_iter().flat_map(|reply|reply.keys).map(|stream| {
            StreamMessage {
                stream: stream.key.to_string(),
                messages: stream.ids.iter()
                    .filter_map(|m|m.map.get("m") )
                    .map(|m|  bytes::Bytes::from_redis_value(m).unwrap().to_vec())
                    .collect(),
                last_id: stream.ids.last()
                    .map(|m| m.id.to_string())
                    .unwrap_or_default()
            }
        }).collect())
    }

    pub async fn add_message(&mut self, room: &str, docid: &str, message: Vec<u8>) -> Result<()> {
        // handle sync step 2 like a normal update message
        if message.len() >= 2 && 
           message[0] == yrs::sync::protocol::MSG_SYNC && 
           message[1] == yrs::sync::protocol::MSG_SYNC_STEP_2 {
            if message.len() < 4 {
                // message does not contain any content, don't distribute
                return Ok(());
            }
            let mut message = message.clone();
            message[1] = yrs::sync::protocol::MSG_SYNC_UPDATE;
            
            let stream_name = compute_redis_room_stream_name(room, docid, &self.prefix);
            //  Here are what the LUA script does:
            // 1. create room stream name if not exists, also add the room stream name to the "redis_worker_stream_name"
            // 2. Associate "pending" consumer to the "redis_worker_stream_name" immediately
            // 3. Add the message to the "stream_name"
            let add_message_script = format!(
                r#"if redis.call('EXISTS', KEYS[1]) == 0 then
                    redis.call('XADD', '{0}', '*', 'compact', KEYS[1])
                    redis.call('XREADGROUP', 'GROUP', '{1}', 'pending', 'STREAMS', '{0}', '>')
                end
                redis.call('XADD', KEYS[1], '*', 'm', ARGV[1])"#, 
                self.redis_worker_stream_name, 
                self.redis_worker_group_name
            );
            let _ = Script::new(add_message_script.as_str())
                .key(&stream_name)
                .arg(message)
                .invoke::<()>(&mut self.redis)?;
        }
        Ok(())
    }

    pub async fn get_state_vector(&self, room: &str, docid: &str) -> Result<Vec<u8>> {
        self.store.retrieve_state_vector(room, docid).await
    }

    pub async fn get_doc(&mut self, room: &str, docid: &str) -> Result<DocResult> {
        info!("getDoc({}, {})", room, docid);
        
        let reply = self.redis.xread_options(
            &[compute_redis_room_stream_name(room, docid, &self.prefix)],
            &["0"], 
            &StreamReadOptions::default()
        )?;

        let ms = extract_messages_from_stream_reply(
            reply,
            &self.prefix
        );
        info!("getDoc({}, {}) - retrieved messages", room, docid);

        let doc_messages = ms.get(room)
            .and_then(|m| m.get(docid));
        
        let doc_state = self.store.retrieve_doc(room, docid).await?;
        info!("getDoc({}, {}) - retrieved doc", room, docid);

        let ydoc = YDoc::new();
        let awareness = Awareness::new(ydoc.clone());
        // awareness.set_local_state(None);
        let mut store_references = vec![];

        if let Some(state) = doc_state {
            let update = state.doc.transact().encode_diff_v1(&StateVector::default());
            let _ = ydoc.transact_mut().apply_update(Update::decode_v1(update.as_slice()).unwrap());

            store_references = state.references;
        }

        let doc_changed = Arc::new(AtomicBool::new(false));
        let doc_changed_clone = doc_changed.clone();
        let _ = ydoc.observe_after_transaction(move |_| {
            doc_changed_clone.store(true, Ordering::Relaxed);
        });

        // Process messages
        if let Some(messages) = doc_messages {
            for m in messages.messages.as_slice() {
                let mut decoder = DecoderV1::new(Cursor::new(m.as_slice()));
                match decoder.read_u64()? {
                    // sync message
                    0 => {
                        if decoder.read_u64()? == 2 {
                            let bytes = decoder.read_buf()?;
                            let update = Update::decode_v1(bytes)?;

                            ydoc.transact_mut().apply_update(update)?;
                        }
                    },
                    // awareness message
                    1 => {
                        let bytes = decoder.read_buf()?;
                        let update = AwarenessUpdate::decode_v1(bytes)?;
                        let _ = awareness.apply_update(update);
                    },
                    _ => {}
                }
            }
        }

        Ok(DocResult {
            ydoc,
            awareness,
            redis_last_id: doc_messages
                .map(|m| m.last_id.to_string())
                .unwrap_or_else(|| "0".to_string()),
            store_references,
            doc_changed:  doc_changed.load(Ordering::Relaxed)
        })
    }

    pub async fn consume_worker_queue(
        &mut self,
        try_claim_count: usize,
        update_callback: impl Fn(&str, &YDoc) -> Result<()>,
    ) -> Result<Vec<(String, String)>> {
        let mut conn = self.redis.get_connection()?;
        let reclaimed: StreamAutoClaimReply  = conn.xautoclaim_options(
            &self.redis_worker_stream_name, 
            &self.redis_worker_group_name,
             &self.consumer_name, 
             self.redis_task_debounce,
             0,
             StreamAutoClaimOptions::default().count(try_claim_count),
            )?;

        let mut tasks = Vec::new();
        for message in reclaimed.claimed {
            if let Some(stream) = message.map.get("compact") {
                let stream = String::from_redis_value(stream)?;
                tasks.push((stream, message.id));
            }
        }

        if tasks.is_empty() {
            info!("No tasks available, pausing..");
            sleep(Duration::from_secs(1)).await;
            return Ok(vec![]);
        }

        info!("Accepted tasks: {:?}", tasks);

        for (stream, task_id) in &tasks {
            let stream = stream.as_str();
            let stream_len: u64 = conn.xlen(stream)?;
            if stream_len == 0 {
                let mut pipe = redis::pipe();
                pipe.atomic()
                    .del(stream).ignore()
                    .xdel(&self.redis_worker_stream_name, &[task_id]).ignore();
                pipe.query(&mut conn)?;
                
                info!("Stream still empty, removing recurring task from queue: {}", stream);
            } else {
                let (room, docid) = decode_redis_room_stream_name(stream, &self.prefix)?;
                
                info!("requesting doc from store");
                let doc_result = self.get_doc(&room, &docid).await?;
                info!(
                    "retrieved doc from store. redisLastId={}, storeRefs={:?}",
                    doc_result.redis_last_id,
                    doc_result.store_references
                );

                let last_id = std::cmp::max(
                    parse_redis_id(&doc_result.redis_last_id),
                    parse_redis_id(task_id)
                );
                if doc_result.doc_changed {
                    info!("doc changed, calling update callback");
                    if let Err(e) = update_callback(&room, &doc_result.ydoc) {
                        error!("Update callback error: {:?}", e);
                    }
                    
                    info!("persisting doc");
                    self.store.persist_doc(&room, &docid, &doc_result.ydoc).await?;
                }

                if doc_result.doc_changed && !doc_result.store_references.is_empty() {
                    self.store.delete_references(&room, &docid, doc_result.store_references).await?;
                }

                let min_id = last_id.saturating_sub(self.redis_min_message_lifetime / 1000);
                
                let mut pipe = redis::pipe();
                pipe.atomic()
                    .cmd("XTRIM").arg(stream).arg("MINID").arg(min_id).ignore()
                    .xadd(
                        &self.redis_worker_stream_name,
                        "*",
                        &[("compact", stream)]
                    ).ignore()
                    .cmd("XREADGROUP")
                        .arg("GROUP")
                        .arg(&self.redis_worker_group_name)
                        .arg("pending")
                        .arg("STREAMS")
                        .arg(&self.redis_worker_stream_name)
                        .arg(">").ignore()
                    .xdel(&self.redis_worker_stream_name, &[task_id]).ignore();
                
                pipe.query(&mut conn)?;

                info!(
                    "Compacted stream: stream={}, taskId={}, newLastId={}",
                    stream, task_id, min_id
                );
            }
        }
        Ok(tasks)
    }
}

// 辅助函数：解析 Redis ID 为数字
fn parse_redis_id(id: &str) -> u64 {
    id.split('-')
        .next()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0)
}

pub fn is_smaller_redis_id(a: &str, b: &str) -> bool {
    let parse_id = |s: &str| {
        let mut parts = s.split('-');
        let first = parts.next().unwrap_or("0").parse::<u64>().unwrap_or(0);
        let second = parts.next().unwrap_or("0").parse::<u64>().unwrap_or(0);
        (first, second)
    };
    
    parse_id(a) < parse_id(b)
}

#[derive(Debug)]
pub struct DocMessages {
    pub last_id: String,
    pub messages: Vec<Vec<u8>>,
}

#[derive(Debug)]
pub struct DocResult {
    pub ydoc: YDoc,
    pub awareness: Awareness,
    pub redis_last_id: String,
    pub store_references: Vec<Reference>,
    pub doc_changed: bool,
}

pub fn extract_messages_from_stream_reply(
    stream_reply: Option<StreamReadReply>,
    prefix: &str,
) -> HashMap<String, HashMap<String, DocMessages>> {
    let mut messages: HashMap<String, HashMap<String, DocMessages>> = HashMap::new();
    
    if let Some(reply) = stream_reply {
        for doc_stream in reply.keys {
            let name =  doc_stream.key;
            if let Ok((room, doc_id)) = decode_redis_room_stream_name(&name, prefix) {
                let doc_messages = messages
                    .entry(room)
                    .or_default()
                    .entry(doc_id)
                    .or_insert_with(|| DocMessages {
                        last_id: doc_stream.ids.last()
                            .map(|m| m.id.to_string())
                            .unwrap_or_default(),
                        messages: Vec::new(),
                    });

                for message in doc_stream.ids {
                    if let Some(content) = message.map.get("m") {
                        let content = String::from_redis_value(content).expect("Invalid message content");
                        doc_messages.messages.push(content.as_bytes().to_vec());
                    }
                }
            }
        }
    }
    
    messages
}

#[derive(Debug)]
pub struct StreamMessage {
    pub stream: String,
    pub messages: Vec<Vec<u8>>,
    pub last_id: String,
}

pub fn compute_redis_room_stream_name(room: &str, docid: &str, prefix: &str) -> String {
    format!("{}:room:{}:{}", 
        prefix,
        urlencoding::encode(room),
        urlencoding::encode(docid)
    )
}

pub fn decode_redis_room_stream_name(redis_key: &str, expected_prefix: &str) -> Result<(String, String)> {
    let re = Regex::new(r"^(.*):room:(.*):(.*)$").unwrap();
    
    let captures = re.captures(redis_key)
        .ok_or_else(|| anyhow!("Invalid stream name format"))?;
    
    let prefix = captures.get(1)
        .ok_or_else(|| anyhow!("Missing prefix"))?
        .as_str();
        
    if prefix != expected_prefix {
        return Err(anyhow!(
            "Malformed stream name! prefix='{}' expectedPrefix='{}', redis_key='{}'",
            prefix, expected_prefix, redis_key
        ));
    }

    let room =  decode(captures.get(2)
        .ok_or_else(|| anyhow!("Missing room"))?
        .as_str())?
        .into_owned();
    let docid  = decode(captures.get(3)
        .ok_or_else(|| anyhow!("Missing docid"))?
        .as_str())?
        .into_owned();
    Ok((room, docid))
}

pub struct WorkerOpts {
    pub try_claim_count: usize,
    pub update_callback: Arc<dyn Fn(&str, &YDoc) -> Result<()> + Send + Sync>,
}

impl Default for WorkerOpts {
    fn default() -> Self {
        Self {
            try_claim_count: 5,
            update_callback: Arc::new(|_, _| Ok(())),
        }
    }
}

pub struct Worker {
    client: Api,
    opts: WorkerOpts,
}

impl Worker {
    pub async fn new(client: Api, opts: WorkerOpts) -> Self {
        info!(
            "Created worker process id={}, prefix={}, min_message_lifetime={}",
            client.consumer_name, client.prefix, client.redis_min_message_lifetime
        );
        Self { client, opts }
    }

    pub async fn run(&mut self) {
        
        loop {
            if let Err(e) = self.client.consume_worker_queue(
                self.opts.try_claim_count,
                self.opts.update_callback.as_ref(),
            ).await {
                error!("Worker error: {:?}", e);
                break;
            }
        }

        info!(
            "Ended worker process id={}",
            self.client.consumer_name
        );
    }
}

pub async fn create_worker(
    store: Arc<Box<dyn Storage>>,
    redis_prefix: String,
    opts: WorkerOpts,
) -> Result<Worker> {
    let client = create_api_client(store, redis_prefix).await?;
    Ok(Worker::new(client, opts).await)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_id_comparison() {
        // Basic comparisons
        assert!(is_smaller_redis_id("1-2", "2-0"));
        assert!(!is_smaller_redis_id("2-0", "1-2"));
        
        // Same first component
        assert!(is_smaller_redis_id("1-5", "1-10"));
        assert!(!is_smaller_redis_id("1-10", "1-5"));
        
        // Missing components
        assert!(is_smaller_redis_id("5", "5-1"));
        assert!(is_smaller_redis_id("3-5", "4"));
        
        // // Edge cases
        assert!(!is_smaller_redis_id("0-0", "0-0"));
        // overflow
    }

    #[cfg(test)]
    use crate::storage::memory::create_memory_storage;
    use std::env;

    #[ignore="Need start up redis manually"]
    #[tokio::test]
    async fn test_create_api_client() {
        env::set_var("REDIS_URL", "redis://127.0.0.1:6379");
        
        // Create memory storage
        let store = Arc::new(Box::new(create_memory_storage(None)) as Box<dyn Storage>);
        
        // Test API client creation
        let result = create_api_client(store, "test_prefix".to_string()).await;
        if let Err(e) = &result { 
            dbg!(&e);
        }
        assert!(result.is_ok());
        
        if let Ok(api) = result {
            assert_eq!(api.prefix, "test_prefix");
            assert_eq!(api.redis_worker_stream_name, "test_prefix:worker");
            assert_eq!(api.redis_worker_group_name, "test_prefix:worker");
        }
    }

    #[ignore="Need start up redis manually"]
    #[tokio::test]
    async fn test_get_messages() {
        env::set_var("REDIS_URL", "redis://127.0.0.1:6379");
        let store = Arc::new(Box::new(create_memory_storage(None)) as Box<dyn Storage>);
        let api = create_api_client(store, "test_prefix".to_string()).await.unwrap();
        
        // Add a test message to Redis stream
        let mut conn = api.redis.get_connection().unwrap();
        let stream_key = "test_prefix:room:test_room:test_doc";
        let _: String = conn.xadd(stream_key, "*", &[("m", b"test_message")]).unwrap();
        
        // Test get_messages
        let messages = api.get_messages(vec![(stream_key.to_string(), "0".to_string())]).await.unwrap();
        
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].stream, stream_key);
        assert_eq!(messages[0].messages.len(), 1);
        assert_eq!(messages[0].messages[0], b"test_message");
        assert!(!messages[0].last_id.is_empty());
        
        // Cleanup
        let _: () = conn.del(stream_key).unwrap();
    }

    #[ignore="Need start up redis manually"]
    #[tokio::test]
    async fn test_add_message() {
        env::set_var("REDIS_URL", "redis://127.0.0.1:6379");
        let store = Arc::new(Box::new(create_memory_storage(None)) as Box<dyn Storage>);
        let mut api = create_api_client(store, "test_prefix".to_string()).await.unwrap();
        
        // Create a sync step 2 message
        let message = vec![
            yrs::sync::protocol::MSG_SYNC,
            yrs::sync::protocol::MSG_SYNC_STEP_2,
            0, 1, 2, 3  // some content
        ];
        
        // Test adding message
        let result = api.add_message("test_room", "test_doc", message).await;
        if let Err(e) = &result {
            dbg!(e);
        }
        assert!(result.is_ok());
    }

    #[ignore="Need start up redis manually"]
    #[tokio::test]
    async fn test_consume_worker_queue() {
        env::set_var("REDIS_URL", "redis://127.0.0.1:6379");
        let store = Box::new(create_memory_storage(None));
        let mut api = create_api_client(Arc::new(store as Box<dyn Storage>), "test_prefix".to_string()).await.unwrap();
        
        // Prepare test data using add_message
        let message = vec![
            yrs::sync::protocol::MSG_SYNC,
            yrs::sync::protocol::MSG_SYNC_STEP_2,
            0, 1, 2, 3  // test content
        ];
        api.add_message("test_room", "test_doc", message).await.unwrap();

        // Create a callback to track if it was called
        let callback_called = Arc::new(AtomicBool::new(false));
        let callback_called_clone = callback_called.clone();
        
        let callback = move |room: &str, _doc: &YDoc| {
            assert_eq!(room, "test_room");
            callback_called_clone.store(true, Ordering::SeqCst);
            Ok(())
        };

        // Test consuming worker queue
        let tasks = api.consume_worker_queue(5, callback).await.unwrap();
        
        // Verify results
        assert!(!tasks.is_empty());
        assert!(callback_called.load(Ordering::SeqCst));
    }
}
