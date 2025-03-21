use std::sync::Arc;
use axum::{Router, body::Body};
use axum_test::{TestServer, TestServerConfig};
use tracing::Level;
use yredis::{
    protocol::encode_sync_step1, server::WebSocketServerConfig, storage::memory::MemoryStorage
};
use yrs::{updates::encoder::Encode, Doc, ReadTxn, Transact};
use tracing_subscriber::FmtSubscriber;

#[tokio::test]
async fn test_websocket_protocol() {
    let server = setup_test_server().await;

    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");

    let mut ws = server
        .get_websocket("/test-room")
        .await
        .into_websocket()
        .await;
   
    // Create a YDoc and encode its state
    let doc = Doc::new();
    let state_vector = doc.transact().state_vector().encode_v1();
    
    // Create sync step1 message and send it
    let sync_msg = encode_sync_step1(&state_vector);
    ws.send_message(sync_msg.into()).await;

    // Receive and verify server response
    let reply = ws.receive_message().await;
    dbg!(reply);
}

async fn setup_test_server() -> TestServer {
    let storage = Box::new(MemoryStorage::new());

    let config = WebSocketServerConfig {
        redis_prefix: "y".to_string(),
        storage: Arc::new(storage),
    };

    let app = yredis::server::create_websocket_handler_router(config);

    TestServer::builder()
        .http_transport()
        .build(app)
        .unwrap()
}