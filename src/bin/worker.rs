use std::sync::Arc;
use yredis::{api::*, storage::{memory::MemoryStorage, postgres::PostgresStorage, Storage}};

#[tokio::main]
async fn main()  -> anyhow::Result<()>{
    dotenvy::dotenv().expect("Failed to load .env file");

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
        update_callback:  Arc::new(|_, _| Ok(()))
    };

    let mut worker = create_worker(storage, redis_prefix, option).await?;
    worker.run().await;

    Ok(())
}