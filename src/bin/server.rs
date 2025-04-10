use std::{str::FromStr, sync::Arc};
use tracing::Level;
use yredis::{server::{self, WebSocketServerConfig}, storage::{memory::MemoryStorage, postgres::PostgresStorage, Storage}};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().expect("Failed to load .env file");

    let log_level = std::env::var("LOG_LEVEL").unwrap_or("info".into());

    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::from_str(log_level.as_str())?)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");

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