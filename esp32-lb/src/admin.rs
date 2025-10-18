use std::net::SocketAddr;
use std::sync::Arc;

use arc_swap::ArcSwap;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, post},
};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};
use uuid::Uuid;

use crate::{Backend, Snapshot};

#[derive(Clone)]
struct AdminState {
    snapshot: Arc<ArcSwap<Snapshot>>,
}

#[derive(Serialize)]
struct BackendStats {
    id: Uuid,
    uri: String,
    health: &'static str,
    avg_response_ms: u64,
}

impl BackendStats {
    fn from_backend(backend: &Backend) -> Self {
        Self {
            id: backend.id(),
            uri: backend.uri().to_string(),
            health: backend.health().as_str(),
            avg_response_ms: backend.avg_latency_ms(),
        }
    }
}

#[derive(Serialize)]
struct StatsResponse {
    generation: u64,
    backends: Vec<BackendStats>,
}

#[derive(Deserialize)]
struct AddBackendRequest {
    uri: String,
}

#[derive(Serialize)]
struct AddBackendResponse {
    backend: BackendStats,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Debug)]
struct AppError {
    status: StatusCode,
    message: String,
}

impl AppError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let body = Json(ErrorResponse {
            error: self.message,
        });
        (self.status, body).into_response()
    }
}

pub(crate) async fn run_admin_server(
    snapshot: Arc<ArcSwap<Snapshot>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let state = AdminState { snapshot };
    let app = Router::new()
        .route("/stats", get(stats))
        .route("/backends", post(add_backend))
        .route("/backends/:id", delete(remove_backend))
        .with_state(state);

    let addr = SocketAddr::from(([127, 0, 0, 1], 3002));
    info!("Starting admin API on: http://{}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn stats(State(state): State<AdminState>) -> Json<StatsResponse> {
    let snap = state.snapshot.load();
    let backends = snap
        .hosts
        .iter()
        .map(|backend| BackendStats::from_backend(backend))
        .collect();

    Json(StatsResponse {
        generation: snap.generation,
        backends,
    })
}

async fn add_backend(
    State(state): State<AdminState>,
    Json(payload): Json<AddBackendRequest>,
) -> Result<(StatusCode, Json<AddBackendResponse>), AppError> {
    let uri: hyper::Uri = payload
        .uri
        .parse()
        .map_err(|_| AppError::new(StatusCode::BAD_REQUEST, "invalid URI"))?;

    let new_backend = Backend::new(uri);
    let stats = BackendStats::from_backend(&new_backend);

    let snapshot = state.snapshot.load();
    let mut hosts = snapshot.hosts.clone();
    hosts.push(new_backend);
    let new_snapshot = Snapshot {
        hosts,
        generation: snapshot.generation + 1,
        max_tries: snapshot.max_tries,
    };
    state.snapshot.store(Arc::new(new_snapshot));
    info!(backend_id=%stats.id, uri=%stats.uri, "added backend");

    Ok((
        StatusCode::CREATED,
        Json(AddBackendResponse { backend: stats }),
    ))
}

async fn remove_backend(
    Path(id): Path<Uuid>,
    State(state): State<AdminState>,
) -> Result<StatusCode, AppError> {
    let snapshot = state.snapshot.load();
    let mut hosts = snapshot.hosts.clone();
    let initial_len = hosts.len();
    hosts.retain(|backend| backend.id() != id);

    if hosts.len() == initial_len {
        warn!(backend_id=%id, "attempted to remove unknown backend");
        return Err(AppError::new(StatusCode::NOT_FOUND, "backend not found"));
    }

    let new_snapshot = Snapshot {
        hosts,
        generation: snapshot.generation + 1,
        max_tries: snapshot.max_tries,
    };
    state.snapshot.store(Arc::new(new_snapshot));
    info!(backend_id=%id, "removed backend");

    Ok(StatusCode::NO_CONTENT)
}
