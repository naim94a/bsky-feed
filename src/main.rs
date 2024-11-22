use std::{ops::Sub, time::Duration};

use sqlx::Executor;
use tracing::{error, info, instrument::WithSubscriber, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod db;
mod feed;
mod firehose;
mod web;

#[tracing::instrument(skip_all)]
async fn flush_cursor(db: &sqlx::Pool<sqlx::Sqlite>, seq: i64) {
    if seq == 0 {
        return;
    }
    if db.execute(sqlx::query!(
        "INSERT INTO subscriber_state VALUES ('hebrewsky', ?) ON CONFLICT (service) DO UPDATE SET cursor=excluded.cursor",
        seq
    )).await.is_ok() {
        // debug!("cursor updated to {seq}");
    }
}

#[tokio::main]
async fn main() {
    let _ = dotenvy::dotenv();
    // console_subscriber::init();
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("tower_http=trace".parse().unwrap())
                .add_directive("hebrewsky=trace".parse().unwrap()),
        )
        .init();

    let ctrl_c = tokio::signal::ctrl_c();

    let state = db::new().await;
    let state_server = state.clone();
    let (last_seq_tx, mut last_seq_rx) = tokio::sync::watch::channel(0);

    let mut last_seq_rx2 = last_seq_rx.clone();
    let db2 = state.db.clone();
    let server = async move {
        let (tx, rx) = flume::bounded(10_000);
        tokio::task::spawn(async move {
            loop {
                let last_cursor = match db2
                    .fetch_optional(sqlx::query!(
                        "SELECT cursor FROM subscriber_state WHERE service='hebrewsky'"
                    ))
                    .await
                {
                    Err(_) | Ok(None) => None,
                    Ok(Some(v)) => {
                        use sqlx::Row;
                        let r: i64 = v.get(0);
                        info!("starting with cursor {r}");
                        Some(r)
                    }
                };
                firehose::collect_firehose_events(tx.clone(), last_cursor)
                    .with_current_subscriber()
                    .await;
            }
        });

        let db = state_server.db.clone();
        tokio::spawn(
            async move {
                let mut last_seq = None;
                let mut interval = tokio::time::interval(Duration::from_secs(10));
                loop {
                    interval.tick().await;
                    let seq = *last_seq_rx2.borrow_and_update();
                    if let Some(last_seq) = last_seq {
                        if seq == last_seq {
                            warn!("sequence number didn't change! seq = {last_seq}");
                            continue;
                        }
                    }

                    let diff = last_seq.map(|last| seq - last);
                    last_seq = Some(seq);
                    flush_cursor(&db, seq).await;
                    if let Some(diff) = diff {
                        info!("cursor updated to {seq}: (diff = {diff})");
                    }
                }
            }
            .with_current_subscriber(),
        );

        let db = state_server.db.clone();
        tokio::spawn(
            async move {
                let mut interval = tokio::time::interval(Duration::from_secs(3600));
                loop {
                    interval.tick().await;
                    info!("starting db maintenance task...");

                    let oldest_timestamp = std::time::UNIX_EPOCH
                        .elapsed()
                        .unwrap()
                        .sub(Duration::from_secs(3600 * 24 * 7))
                        .as_secs() as i64;

                    match sqlx::query!("DELETE FROM post WHERE indexed_dt < ? OR (created_at is not NULL AND created_at < ?)", oldest_timestamp, oldest_timestamp)
                        .execute(&db).await {
                            Err(err) => {
                                error!("failed to execute maintenance task: {err}");
                            },
                            Ok(r) => {
                                let rows = r.rows_affected();
                                info!("cleaned up {} rows", rows);
                            }
                        }
                }
            }
            .with_current_subscriber(),
        );

        let mut workers = vec![];
        // create workers in order not to exhaust the heap with tasks...
        for _ in 0..32 {
            let last_seq_tx = last_seq_tx.clone();
            let rx = rx.clone();
            let state_server = state_server.clone();
            let h = tokio::spawn(
                async move {
                    while let Ok(evt) = rx.recv_async().await {
                        let seq = firehose::process_firehose_blob(state_server.clone(), evt)
                            .with_current_subscriber()
                            .await;
                        if let Some(seq) = seq {
                            last_seq_tx.send_modify(|v| *v = seq);
                        }
                    }
                }
                .with_current_subscriber(),
            );
            workers.push(h);
        }
        let mut dur = tokio::time::interval(Duration::from_secs(1));
        loop {
            let _ = dur.tick().await;
            let jobs = rx.len();
            if jobs > 0 {
                info!("{jobs:5} jobs queued");
            }
        }
    };
    let webservice = web::web_server(state.clone());

    tokio::select! {
        _ = ctrl_c => {
            info!("shutting down...");
        },
        _ = server => {
            error!("this is unexpected");
        },
        _ = webservice => {
            error!("this is unexpected");
        }
    }

    flush_cursor(&state.db, *last_seq_rx.borrow_and_update()).await;
    state.db.close().await;
    info!("bye.");
}
