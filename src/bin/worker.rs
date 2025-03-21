use std::{str::FromStr, sync::Arc};
use tracing::Level;
use yredis::{api::*, storage::{memory::MemoryStorage, postgres::PostgresStorage, Storage}};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main()  -> anyhow::Result<()>{
    dotenvy::dotenv().expect("Failed to load .env file");

    let log_level = std::env::var("LOG_LEVEL").unwrap_or("info".into());

    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::from_str(log_level.as_str()))
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");

    let storage_url = &std::env::var("DATABASE_URL").unwrap_or("memory:".into());

    let storage = if storage_url.starts_with("postgres") {
        Box::new(PostgresStorage::new(storage_url).await?)  as Box<dyn Storage>
    } else {
        // Always fallback to memory storage
        Box::new(MemoryStorage::new()) as Box<dyn Storage>
    };

    let redis_prefix = std::env::var("REDIS_PREFIX").unwrap_or("y".to_owned());
    let storage =  Arc::new(storage);

    let option = WorkerOpts {
        try_claim_count: 5,
        update_callback:  Arc::new(|_, _| {
            //TODO: your update callback
            Ok(())
        })
    };

    let mut worker = create_worker(storage, redis_prefix, option).await?;
    worker.run().await;

    Ok(())
}