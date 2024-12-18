use std::sync::Arc;

use crate::db::State;
use atproto_feedgen::FeedManager;
use atrium_api::app::bsky::feed::get_feed_skeleton;
use atrium_api::types::string::Did;
use hyper::header::CONTENT_TYPE;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use tower_http::compression::CompressionLayer;
use tower_http::trace::TraceLayer;
use tracing::debug;
use tracing::{error, trace};

use axum::async_trait;
use axum::extract::FromRequestParts;
use base64::Engine;
use hyper::header;

#[derive(Serialize)]
#[serde(untagged)]
enum GetFeedSkeletonResult {
    Ok(get_feed_skeleton::OutputData),
    Err(get_feed_skeleton::Error),
}

#[derive(Deserialize, Debug)]
struct JwtPayload {
    iat: u32,
    iss: String,
    aud: String,
    exp: u32,
    lxm: String,
    jti: String,
}

#[derive(Deserialize, Debug)]
struct JwtHeader {
    typ: String,
    alg: String,
}

struct AuthExtractor;
#[async_trait]
impl<S> FromRequestParts<S> for AuthExtractor {
    type Rejection = ();

    async fn from_request_parts(
        req: &mut axum::http::request::Parts,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        let auth = req.headers.get(header::AUTHORIZATION).ok_or(())?;
        let auth = auth.to_str().map_err(|_| ())?;
        let auth = auth.strip_prefix("Bearer ").ok_or(())?;
        let auth = auth.trim();
        let mut auth = auth.split('.');
        let header = auth.next().ok_or(())?;
        let payload = auth.next().ok_or(())?;
        let sig = auth.next().ok_or(())?;
        if auth.next().is_some() {
            return Err(());
        }

        // let header = serde_json::from_str::<JwtHeader>(header).map_err(|_| ())?;

        let payload = {
            let mut res = vec![];
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode_vec(payload, &mut res)
                .map_err(|_| ())?;
            res
        };
        let payload = serde_json::from_slice::<JwtPayload>(&payload).map_err(|_| ())?;
        trace!("current token = {:?}", &payload);

        //        tracing::info!(" auth = {auth} ");
        Ok(Self)
    }
}

async fn get_feed_skeleton(
    axum::extract::State(feed_mgr): axum::extract::State<Arc<FeedManager<sqlx::SqlitePool>>>,
    axum::extract::Query(q): axum::extract::Query<get_feed_skeleton::ParametersData>,
    _auth: Option<AuthExtractor>,
) -> axum::Json<GetFeedSkeletonResult> {
    let requested_feed = q.feed.as_str();
    let requested_feed = match requested_feed.strip_prefix(feed_mgr.get_feed_prefix()) {
        Some(v) => v,
        None => {
            debug!("unknown feed requested: {requested_feed}");
            return GetFeedSkeletonResult::Err(get_feed_skeleton::Error::UnknownFeed(None)).into();
        }
    };
    let limit = q.limit.map(|v| u8::from(v)).unwrap_or(30) as u32;

    let posts = feed_mgr.fetch_posts(requested_feed, None, limit, q.cursor);
    match posts.await {
        Ok(v) => GetFeedSkeletonResult::Ok(v).into(),
        Err(e) => GetFeedSkeletonResult::Err(e).into(),
    }
}

pub async fn describe_feed_generator(
    axum::extract::State(feed_mgr): axum::extract::State<Arc<FeedManager<SqlitePool>>>,
) -> axum::response::Response {
    let body = axum::body::Body::from(feed_mgr.describe());
    axum::response::Response::builder()
        .header(CONTENT_TYPE, "application/json")
        .body(body)
        .expect("failed to create response")
}

async fn well_known_did(
    axum::extract::State(feed_mgr): axum::extract::State<Arc<FeedManager<SqlitePool>>>,
) -> axum::response::Response {
    let body = axum::body::Body::from(feed_mgr.well_known_did());
    axum::response::Response::builder()
        .header(CONTENT_TYPE, "application/json")
        .body(body)
        .expect("failed to create response")
}

pub async fn web_server(state: State) {
    let feedgen_hostname = std::env::var("FEEDGEN_HOSTNAME").unwrap();
    let owner_did = std::env::var("OWNER_DID").unwrap();
    let x = state.db.clone();
    let mut feed_mgr = FeedManager::new(
        Did::new(format!("did:web:{feedgen_hostname}")).unwrap(),
        Did::new(owner_did).unwrap(),
        &feedgen_hostname,
        x,
    );
    feed_mgr.register_feed(
        "hebrew-feed",
        |db, _user, limit, start_cursor| {
            let start_cursor = start_cursor.map(|v| {
                i64::from_str_radix(&v, 10).ok()
            }).flatten();
            Box::pin(
                async move {
                let mut cursor = None;
                let mut feed = vec![];
                match sqlx::query!(
                    r#"SELECT repo, post_path, indexed_dt as "indexed: i64" FROM post WHERE (? is NULL OR indexed_dt < ?) ORDER BY (indexed_dt/60) DESC, created_at DESC LIMIT ?"#,
                    start_cursor,
                    start_cursor,
                    limit
                )
                .fetch_all(&db)
                .await
                {
                    Err(err) => {
                        error!("db error: {err}");
                    }
                    Ok(rows) => {
                        feed = rows
                            .into_iter()
                            .map(|row| {
                                cursor = row.indexed;
                                let post = atrium_api::app::bsky::feed::defs::SkeletonFeedPostData {
                                    feed_context: None,
                                    post: format!("at://{}/app.bsky.feed.post/{}", row.repo, row.post_path),
                                    reason: None,
                                };
                                post.into()
                            })
                            .collect::<Vec<_>>();
                    }
                }
                if limit > feed.len() as u32 || cursor == start_cursor {
                    cursor = None;
                }
                Ok(get_feed_skeleton::OutputData {
                    cursor: cursor.map(|v| v.to_string()),
                    feed,
                })
            })
        },
        false,
    );
    let feed_mgr = Arc::new(feed_mgr);

    let app = axum::Router::new()
        .route(
            "/xrpc/app.bsky.feed.getFeedSkeleton",
            axum::routing::get(get_feed_skeleton),
        )
        .route(
            "/xrpc/app.bsky.feed.describeFeedGenerator",
            axum::routing::get(describe_feed_generator),
        )
        .route("/.well-known/did.json", axum::routing::get(well_known_did))
        .layer(CompressionLayer::new())
        .layer(TraceLayer::new_for_http())
        .with_state(feed_mgr);

    axum::serve(
        tokio::net::TcpListener::bind("0.0.0.0:80").await.unwrap(),
        app,
    )
    .await
    .unwrap();
}
