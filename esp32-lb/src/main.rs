use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::{convert::Infallible, net::SocketAddr};

use arc_swap::ArcSwap;
use bytes::Bytes;
use std::io::IsTerminal;
use tokio::time::{Duration, Instant, timeout};

use hyper::server::conn::http1;
use hyper::{Request, Response, Uri, body::Incoming, service::service_fn};
use hyper_util::client::legacy::{Client, connect::HttpConnector};
use hyper_util::rt::TokioExecutor;
use hyper_util::rt::TokioIo;

use uuid::Uuid;

use http_body_util::{BodyExt, Full, combinators::BoxBody};

use tokio::net::TcpListener;

use tracing::{debug, error, info, warn};
use tracing_appender::non_blocking::{NonBlocking, WorkerGuard};
use tracing_subscriber::{EnvFilter, fmt};

mod admin;

// TODO: can't update hosts dynamically
// TODO: not keeping avg delay
// TODO: TUI

type BxBody = BoxBody<Bytes, hyper::Error>;

type ReqBody = Full<Bytes>; // what you send upstream
type RespBody = hyper::body::Incoming; // what you receive from hyper
type HttpClient = Client<HttpConnector, ReqBody>;

pub fn init_tracing() -> WorkerGuard {
    let (nb, guard): (NonBlocking, WorkerGuard) = tracing_appender::non_blocking(std::io::stdout());

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let is_tty = std::io::stdout().is_terminal();

    fmt()
        .with_env_filter(filter)
        .with_writer(nb)
        .with_ansi(is_tty)
        .compact()
        .init();

    guard
}

// ----- Health state (repr u8 so we can store in an AtomicU8) -----
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Health {
    Healthy = 0,
    Unhealthy = 1, // not directly used in selection; kept for completeness
    Probe = 2,
    Disabled = 3,
}

impl From<u8> for Health {
    fn from(v: u8) -> Self {
        match v {
            0 => Health::Healthy,
            1 => Health::Unhealthy,
            2 => Health::Probe,
            3 => Health::Disabled,
            _ => Health::Disabled,
        }
    }
}

impl Health {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Health::Healthy => "Healthy",
            Health::Unhealthy => "Unhealthy",
            Health::Probe => "Probe",
            Health::Disabled => "Disabled",
        }
    }
}

struct Metrics {
    avg_ns: AtomicU64,
}

impl Metrics {
    fn new() -> Self {
        Self {
            avg_ns: AtomicU64::new(0)
        }
    }
    fn record(&self, d: Duration) {
        let ns = d.as_nanos() as u64;
        let old = self.avg_ns.load(Ordering::Relaxed);
        let new = if old == 0 {
            ns
        } else {
            old + ((ns.saturating_sub(old)) >> 3)
        };
        self.avg_ns.store(new, Ordering::Relaxed);
    }
    fn avg_duration(&self) -> Duration {
        Duration::from_nanos(self.avg_ns.load(Ordering::Relaxed))
    }
}

pub(crate) struct Backend {
    id: Uuid,
    uri: Uri,
    health: AtomicU8,
    metrics: Metrics,
}

impl Backend {
    pub(crate) fn new(uri: Uri) -> Arc<Self> {
        Arc::new(Self {
            id: Uuid::new_v4(),
            uri,
            health: AtomicU8::new(Health::Healthy as u8),
            metrics: Metrics::new(),
        })
    }
    #[inline]
    pub(crate) fn health(&self) -> Health {
        Health::from(self.health.load(Ordering::Relaxed))
    }
    pub(crate) fn set_health(&self, h: Health) {
        self.health
            .store(h as u8, std::sync::atomic::Ordering::Relaxed);
    }
    fn record_latency(&self, d: Duration) {
        self.metrics.record(d);
    }
    pub(crate) fn avg_latency_ms(&self) -> u64 {
        self.metrics.avg_ns.load(Ordering::Relaxed) / 1_000_000
    }
    pub(crate) fn uri(&self) -> &Uri {
        &self.uri
    }
    pub(crate) fn id(&self) -> Uuid {
        self.id
    }
}

pub(crate) struct Snapshot {
    pub(crate) hosts: Vec<Arc<Backend>>,
    pub(crate) generation: u64,
    pub(crate) max_tries: u8,
}

struct Req {
    id: Uuid,
    retry: AtomicU8,
}

impl Req {
    fn new() -> Req {
        Req {
            id: Uuid::new_v4(),
            retry: AtomicU8::new(0),
        }
    }
}

fn build_outgoing<B>(mut req: Request<()>, body: B, target: &Uri) -> Request<B> {
    // rewrite scheme+authority; keep original path+query
    let mut parts = req.uri().clone().into_parts();
    parts.scheme = target.scheme().cloned();
    parts.authority = target.authority().cloned();
    if parts.path_and_query.is_none() {
        parts.path_and_query = Some("/".parse().unwrap());
    }
    *req.uri_mut() = Uri::from_parts(parts).unwrap();

    Request::from_parts(req.into_parts().0, body)
}

async fn handle(
    req: Request<Incoming>,
    client: HttpClient,
    snap_holder: Arc<ArcSwap<Snapshot>>,
    rr_cursor: Arc<AtomicU64>,
    curr_req: Req,
) -> Result<Response<BxBody>, Infallible> {
    // For GET/HEAD/etc. this is tiny; for big uploads, consider streaming w/o retries.
    let (parts, body_stream) = req.into_parts();
    let body_bytes = match body_stream.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(_) => {
            let body = Full::new(Bytes::from_static(b"Bad request body"))
                .map_err(|never: Infallible| match never {})
                .boxed();
            return Ok(Response::builder().status(400).body(body).unwrap());
        }
    };
    let req_without_body = Request::from_parts(parts, ());

    // ---- Retry loop ----
    let snap_arc = snap_holder.load();
    let snap = snap_arc.as_ref();

    let max_tries = snap.max_tries as usize;
    let mut last_err: Option<String> = None;

    for attempt in 0..max_tries {
        // Pick a backend (advance cursor each attempt)
        let backend = match round_robin(snap, &rr_cursor, snap.hosts.len()) {
            Some(b) => b,
            None => {
                let body = Full::new(Bytes::from_static(b"No backends available\n"))
                    .map_err(|never: Infallible| match never {})
                    .boxed();
                return Ok(Response::builder().status(503).body(body).unwrap());
            }
        };

        // Rebuild request with cloned body
        let outgoing: Request<ReqBody> = build_outgoing(
            req_without_body.clone(),
            Full::new(Bytes::from(body_bytes.clone())),
            backend.uri(),
        );

        // This now matches the client's expected request type:
        let start_time = Instant::now();
        let attempt_res = timeout(Duration::from_secs(5), client.request(outgoing)).await;
        let end_time = Instant::now();

        let elapsed_duration = end_time.duration_since(start_time);

        backend.record_latency(elapsed_duration);

        debug!(
            req_id = %curr_req.id,
            attempt,
            backend = %backend.uri(),
            msg = "proxy attempt",
            backend_avg = %format!(
                "{} ms",
                backend.avg_latency_ms()
            ),
        );

        match attempt_res {
            Err(_) => {
                // timed out
                backend.set_health(Health::Probe);
                last_err = Some("timeout".into());
                continue;
            }
            Ok(Err(e)) => {
                // connect/reset error, etc.
                backend.set_health(Health::Probe);
                last_err = Some(format!("transport error: {e}"));
                continue;
            }
            Ok(Ok(resp)) => {
                // Optional: Retry on certain upstream statuses (e.g., 502/503/504)
                let status = resp.status();
                if matches!(status.as_u16(), 502 | 503 | 504) && attempt + 1 < max_tries {
                    warn!(req_id=%curr_req.id, attempt, status=%status.as_u16(), "upstream error, will retry");
                    backend.set_health(Health::Probe);
                    last_err = Some(format!("upstream {}", status));
                    continue;
                }

                // Success (or we accept the status)
                return Ok(resp.map(|b| b.boxed()));
            }
        }
    }

    // Exhausted attempts
    error!(req_id=%curr_req.id, err=?last_err, "dropping after retries");
    let body = Full::new(Bytes::from_static(b"Bad Gateway"))
        .map_err(|never: Infallible| match never {})
        .boxed();
    Ok(Response::builder().status(502).body(body).unwrap())
}

fn round_robin<'a>(
    snap: &'a Snapshot,
    rr_cursor: &AtomicU64,
    max_skips: usize,
) -> Option<&'a Arc<Backend>> {
    let n = snap.hosts.len();
    if n == 0 {
        return None;
    }
    let start = rr_cursor.fetch_add(1, Ordering::Relaxed) as usize;
    for i in 0..std::cmp::min(max_skips, n) {
        let idx = (start + i) % n;
        let b = &snap.hosts[idx];
        match b.health() {
            Health::Healthy => {
                return Some(b);
            }
            _ => {
                debug!(msg="couldn't pick backend", backend=%b.id());
                continue;
            }
        }
    }
    None
}

async fn run_proxy(
    addr: SocketAddr,
    snapshot_holder: Arc<ArcSwap<Snapshot>>,
    rr_cursor: Arc<AtomicU64>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind(addr).await?;
    info!("Running server on: http://{}", addr);
    let client: HttpClient = Client::builder(TokioExecutor::new()).build(HttpConnector::new());

    loop {
        let (stream, peer_addr) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let client = client.clone();
        let snapshot_holder = snapshot_holder.clone();
        let rr_cursor = rr_cursor.clone();

        tokio::spawn(async move {
            let svc = service_fn(move |req| {
                let curr_req = Req::new();
                let client = client.clone();
                let snapshot_holder = snapshot_holder.clone();
                let rr_cursor = rr_cursor.clone();
                let peer_addr = peer_addr;

                info!(client=%peer_addr, req_id=%curr_req.id, method=%req.method(), path=%req.uri().path(), "handling request");

                async move { handle(req, client, snapshot_holder, rr_cursor, curr_req).await }
            });
            if let Err(e) = http1::Builder::new().serve_connection(io, svc).await {
                tracing::error!(error=?e, "serve error");
            }
        });
    }
    #[allow(unreachable_code)]
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let _guard = init_tracing();
    let hosts = vec![
        Backend::new("http://127.0.0.1:8080".parse().unwrap()),
        Backend::new("http://127.0.0.1:8081".parse().unwrap()),
        Backend::new("http://127.0.0.1:8082".parse().unwrap()),
    ];
    let initial = Snapshot {
        hosts,
        generation: 0,
        max_tries: 3,
    };
    let snapshot_holder = Arc::new(ArcSwap::from_pointee(initial));
    let rr_cursor = Arc::new(AtomicU64::new(0));
    let proxy_addr = SocketAddr::from(([127, 0, 0, 1], 3001));

    tokio::try_join!(
        run_proxy(proxy_addr, snapshot_holder.clone(), rr_cursor.clone()),
        admin::run_admin_server(snapshot_holder.clone()),
    )?;

    Ok(())
}
