use axum::{
    extract::{ws::WebSocket, Path, Query, State, WebSocketUpgrade},
    response::IntoResponse,
    routing::get,
    Router,
};
use tokio::sync::RwLock;
use std::{collections::HashMap, sync::Arc};

use crate::{storage::Storage, ws::{make_websocket_server, User}};

pub struct WebSocketServerConfig {
    pub redis_prefix: String,
    pub storage: Arc<Box<dyn Storage>>,
}

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<WebSocketServerConfig>,
}

async fn websocket_handler(
    ws: WebSocketUpgrade,
    Path(room): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    //TODO: Handle authentication yourself
    // let token = params.get("yauth")
    //     .ok_or_else(|| anyhow::anyhow!("Missing token"))?;
        
    let user = Arc::new(RwLock::new(User::new(room, true, "test_user".to_owned())));

    ws.on_upgrade(move |socket| async move {
        if let Err(e) = handle_socket(socket, user, &state).await {
            eprintln!("YRedis WebSocket error: {}", e);
        }
    })
}

pub async fn handle_socket(
    socket: WebSocket,
    user: Arc<RwLock<User>>,
    state: &AppState,
) -> anyhow::Result<()> {
    let store = state.config.storage.clone();
  
    let ws_server = make_websocket_server(
        store.clone(),
        state.config.redis_prefix.as_str()
    ).await?;

    ws_server.handle(user, socket, |room, index, client| {
        Ok(())
    }).await
}

pub async fn run(router: Router, port: u16) -> anyhow::Result<()> {
    let addr = format!("0.0.0.0:{}", port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}


pub fn create_websocket_handler_router(
    config: WebSocketServerConfig,
) -> Router {
    let state = AppState {
        config: Arc::new(config),
    };

    let router = Router::new()
        .route("/{room}", get(websocket_handler))
        .with_state(state);

    router
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to install CTRL+C handler");
}