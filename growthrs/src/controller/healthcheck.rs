use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{Router, routing::get};

async fn healthcheck_handler() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}

pub async fn healthcheck() -> anyhow::Result<()> {
    let app = Router::new()
        .route("/healthz", get(healthcheck_handler))
        .route("/readyz", get(healthcheck_handler));
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();
    axum::serve(listener, app).await?;
    Ok(())
}
