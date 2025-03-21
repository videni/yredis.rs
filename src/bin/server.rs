use std::sync::Arc;

use yredis::{server::{self, WebSocketServerConfig}, storage::{memory::MemoryStorage, postgres::PostgresStorage, Storage}};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().expect("Failed to load .env file");

    let port = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(3002);

    let storage_url = &std::env::var("DATABASE_URL").unwrap_or("memory:".into());

    let storage = if storage_url.starts_with("postgres") {
        Box::new(PostgresStorage::new(storage_url).await?)  as Box<dyn Storage>
    } else {
        // Always fallback to memory storage
        Box::new(MemoryStorage::new()) as Box<dyn Storage>
    };

    let config = WebSocketServerConfig {
        redis_prefix: std::env::var("REDIS_PREFIX").unwrap_or("y".to_owned()),
        storage: Arc::new(storage),
    };

    let router  = server::create_websocket_handler_router(config);

    server::run(router, port).await
}