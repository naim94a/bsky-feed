#![deny(unused_crate_dependencies)]

use std::{ops::Sub, sync::Arc, time::Duration};

use atproto_feedgen::{self, Collections, Firehose, MessageTypes};
use sqlx::Executor;
use tokio::sync::Mutex;
use tracing::{error, info, instrument::WithSubscriber, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod db;
mod feed;
mod web;

struct FirehoseProcessor {
    db: sqlx::SqlitePool,
    last_cursor_rx: Mutex<tokio::sync::watch::Receiver<i64>>,
    last_cursor_tx: tokio::sync::watch::Sender<i64>,
}

impl FirehoseProcessor {
    pub fn new(db: sqlx::SqlitePool) -> Self {
        let (last_cursor_tx, last_cursor_rx) = tokio::sync::watch::channel(0);
        Self {
            db,
            last_cursor_rx: tokio::sync::Mutex::new(last_cursor_rx),
            last_cursor_tx,
        }
    }

    async fn get_last_cursor(&self) -> i64 {
        let mut rx = self.last_cursor_rx.lock().await;
        let v = rx.borrow_and_update();
        *v
    }
}

struct AtUriParts<'a> {
    repo: &'a str,
    path: &'a str,
}
impl core::fmt::Display for AtUriParts<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "at://{}/{}", self.repo, self.path)
    }
}

impl atproto_feedgen::FirehoseHandler for FirehoseProcessor {
    async fn get_last_cursor(&self) -> Option<i64> {
        match self
            .db
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
        }
    }

    async fn update_cursor(&self, cursor: i64) {
        self.last_cursor_tx.send_modify(|v| *v = cursor);
    }

    async fn on_repo_operation(&self, op: atproto_feedgen::RepoCommitOp) {
        match op {
            atproto_feedgen::RepoCommitOp::Create(commit) => match commit.record {
                atrium_api::record::KnownRecord::AppBskyFeedPost(post) => {
                    let atp = AtUriParts {
                        repo: &commit.repo,
                        path: &commit.path,
                    };
                    if let Some(cid) = &commit.cid {
                        let post = post.data;
                        feed::process_post(&self.db, atp, cid, post).await;
                    } else {
                        panic!("no cid!");
                    }
                }
                _ => {}
            },
            atproto_feedgen::RepoCommitOp::Delete(commit)
                if commit.collection == Collections::AppBskyFeedPost =>
            {
                let atp = AtUriParts {
                    repo: &commit.repo,
                    path: &commit.path,
                };
                feed::delete_post(&self.db, atp).await;
            }
            _ => {}
        }
    }
}

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

    let firehose_handler = Arc::new(FirehoseProcessor::new(state.db.clone()));
    let firehose = Arc::new(
        Firehose::new("bsky.network", firehose_handler.clone())
            .accept_message_types(MessageTypes::Commit | MessageTypes::Info)
            .accept_collections(Collections::AppBskyFeedPost),
    );

    let server_firehose = firehose.clone();
    let server_fh = firehose_handler.clone();
    let server = async move {
        tokio::task::spawn(async move {
            loop {
                server_firehose.collect_firehose_events().await;
                warn!("firehose error; will reconnect.");
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        });

        let db = state_server.db.clone();
        tokio::spawn(
            async move {
                let mut last_seq = None;
                let mut interval = tokio::time::interval(Duration::from_secs(10));
                loop {
                    interval.tick().await;
                    let seq = server_fh.get_last_cursor().await;
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
                        .sub(Duration::from_secs(3600 * 24 * 30))
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
                    match sqlx::query!("VACUUM").execute(&db).await {
                        Ok(_) => {
                            info!("vacuum done")
                        },
                        Err(err) => {
                            error!("failed to vacuum db: {err}");
                        },
                    }
                }
            }
            .with_current_subscriber(),
        );

        // create workers in order not to exhaust the heap with tasks...
        let mut workers = vec![];
        for _ in 0..8 {
            let firehose = firehose.clone();
            let h = tokio::spawn(async move {
                firehose.process_firehose_event().await;
            });
            workers.push(h);
        }
        let mut dur = tokio::time::interval(Duration::from_secs(1));
        loop {
            let _ = dur.tick().await;
            let jobs = firehose.get_queue_pending();
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

    let cursor = firehose_handler.get_last_cursor().await;
    flush_cursor(&state.db, cursor).await;
    state.db.close().await;
    info!("bye.");
}
