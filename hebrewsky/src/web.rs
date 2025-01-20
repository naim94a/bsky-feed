use std::sync::Arc;

use crate::db::State;
use crate::utils;
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

struct AuthExtractor {
    iss: String,
    handle: Option<String>,
}

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
        let mut auth = auth.rsplitn(2, '.');
        let header_and_payload = auth.next().ok_or(())?;
        let sig = auth.next().ok_or(())?;
        let (header, payload) = {
            let mut s = header_and_payload.split('.');
            let h = s.next().ok_or(())?;
            let p = s.next().ok_or(())?;
            if s.next().is_some() {
                return Err(());
            }
            (h, p)
        };

        let header = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(&header)
            .map_err(|_| ())?;
        let header = serde_json::from_slice::<JwtHeader>(&header).map_err(|_| ())?;

        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(&payload)
            .map_err(|_| ())?;

        let payload = serde_json::from_slice::<JwtPayload>(&payload).map_err(|_| ())?;
        trace!("token: header={header:?} payload={payload:?}");
        let timestamp = std::time::UNIX_EPOCH.elapsed().unwrap().as_secs();
        if timestamp > payload.exp as u64 {
            trace!("JWT is expired");
            return Err(());
        }

        let did_doc = match utils::resolve_did(&payload.iss).await {
            Ok(v) => v,
            Err(_) => {
                trace!("failed to resolve did {}", &payload.iss);
                return Err(());
            }
        };
        if let Some(vm) = did_doc.verification_method {
            for verification_method in vm {
                if let Some(mb) = verification_method.public_key_multibase {
                    let did_key = format!("did:key:{mb}");
                    if atrium_crypto::verify::verify_signature(
                        &did_key,
                        header_and_payload.as_bytes(),
                        sig.as_bytes(),
                    )
                    .is_ok()
                    {
                        trace!("JWT token valid");
                        let handle = did_doc
                            .also_known_as
                            .map(|mut v| {
                                if v.is_empty() {
                                    None
                                } else {
                                    Some(v.remove(0))
                                }
                            })
                            .flatten();
                        return Ok(Self {
                            iss: payload.iss,
                            handle,
                        });
                    }
                }
            }
        } else {
            debug!("did doc has no verification method");
        }
        return Err(());
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
            // Our cursor contains two parts: 1. The highest indexed_dt of the view. 2. The indexed_dt to start from.
            // The first part is used to determine the score's time decay component.
            // The second part allows us to using pagination.
            Box::pin(
                async move {
                    let start_cursor = match start_cursor.map(|v| {
                        let (cursor_start_time, cursor_offset) = v.split_once('_')?;
                        let cursor_start_time = cursor_start_time.parse::<i64>().ok()?;
                        let cursor_offset = cursor_offset.parse::<i64>().ok()?;
                        Some((cursor_start_time, cursor_offset))
                    }).flatten() {
                        Some(v) => v,
                        None => {
                            let res = sqlx::query!(r#"WITH x AS (
                                SELECT MAX(indexed_dt) AS m FROM post
                                UNION
                                SELECT MAX(unixepoch(indexed_dt)) AS m FROM interactions
                                )
                                SELECT MAX(m) AS "latest!" FROM x"#)
                                .fetch_one(&db).await;
                            match res {
                                Ok(v) => (v.latest, v.latest),
                                Err(_) => return Err(get_feed_skeleton::Error::UnknownFeed("DB Error".to_owned().into())),
                            }
                        }
                    };
                // let mut cursor = None;
                // let mut feed = vec![];
                /*
                Ranking posts:
                1. Time decay: indexed_dt

                2. Likes & Repost: SELECT COUNT(*) FROM interactions WHERE interaction_type = '...';
                    quote +21
                    repost +20
                    like +10
                3. Replies: SELECT COUNT(*) FROM post WHERE (reply_to = '...' OR reply_root = '...') AND repo != self.repo;
                    reply +15
                4. Is post a reply: post.reply_to is NULL;
                    root post +20
                5. Rate limit: count of the amout of root posts from the same user in the last 6 hours of the current post time.
                    rate_limit -12*n

                rank = indexed_dt + 25*quote + 20*repost + 10*like + 15*reply + 20*root_post - 12*rate_limit
                */
                // TODO: put likes, reposts and quotes into an indexed view.
                let min_dt = start_cursor.0 - 3600i64*24*7;
                let rows = sqlx::query!(r#"
                    WITH likes AS (
                        SELECT target_repo AS repo, target_path AS path, count(*) AS likes
                        FROM interactions
                        WHERE interaction_type = 'like' AND indexed_dt >= ? AND indexed_dt <= ?
                        GROUP BY target_repo, target_path
                    ),
                    reposts AS (
                        SELECT target_repo AS repo, target_path AS path, count(*) AS reposts
                        FROM interactions
                        WHERE interaction_type = 'repost' AND indexed_dt >= ? AND indexed_dt <= ?
                        GROUP BY target_repo, target_path
                    ),
                    quotes AS (
                        SELECT target_repo AS repo, target_path AS path, count(*) AS quotes
                        FROM interactions
                        WHERE interaction_type = 'quote' AND indexed_dt >= ? AND indexed_dt <= ?
                        GROUP BY target_repo, target_path
                    ),
                    replies AS (
                        SELECT reply_to AS path, count(*) AS replies
                        FROM post
                        WHERE reply_to IS NOT NULL AND indexed_dt >= ? AND indexed_dt <= ? AND reply_to NOT LIKE concat('at://', repo, '/%')
                        GROUP BY reply_to
                    ),
                    ranks AS (
                        SELECT
                            p.repo as repo,
                            p.post_path as post_path,
                            p.indexed_dt as indexed_dt,
                            (IIF(p.reply_root is NULL, 2.0, 1.0) + IIF(? - p.indexed_dt < 300, 1.0, 0.0) + 0.5*coalesce(l.likes, 0.0) + 1.0*coalesce(rp.replies, 0.0) + 2.0*coalesce(r.reposts, 0.0) + 2.1*coalesce(q.quotes, 0.0))/EXP(CAST(((? - p.indexed_dt) / 3600) AS FLOAT) / 2.0) AS rank
                        FROM post p
                        LEFT JOIN likes l ON (l.repo = p.repo AND l.path = p.post_path)
                        LEFT JOIN reposts r ON (r.repo = p.repo AND r.path = p.post_path)
                        LEFT JOIN quotes q ON (q.repo = p.repo AND q.path = p.post_path)
                        LEFT JOIN replies rp ON (rp.path = concat('at://', p.repo, '/app.bsky.feed.post/', p.post_path))
                        WHERE p.indexed_dt >= ? AND p.indexed_dt <= ?
                            AND abs(p.indexed_dt - p.created_at) < 3600*12
                    )
                    SELECT repo, post_path, rank as "rank!: f64", indexed_dt as "indexed_dt!: i64" FROM ranks
                    WHERE indexed_dt >= ? AND indexed_dt <= ?
                    ORDER BY rank DESC, indexed_dt DESC
                    LIMIT ?
                    "#,
                        // for with clauses for ranking.
                        min_dt, start_cursor.0, // likes
                        min_dt, start_cursor.0, // reposts
                        min_dt, start_cursor.0, // quotes
                        min_dt, start_cursor.0, // replies

                        // ranks SELECT clause
                        start_cursor.0,
                        start_cursor.0,

                        min_dt, start_cursor.0, // ranks WHERE clause

                        // use for the final query
                        min_dt, start_cursor.1, limit)
                    .fetch_all(&db)
                        .await;
                let rows = match rows {
                    Err(err) => {
                        error!("db error: {err}");
                        return Err(get_feed_skeleton::Error::UnknownFeed("DB Error".to_owned().into()));
                    }
                    Ok(rows) => rows,
                };
                let mut last_dt = start_cursor.1;
                let feed = rows.into_iter().map(|row| {
                    let post = atrium_api::app::bsky::feed::defs::SkeletonFeedPostData {
                        feed_context: None,
                        post: format!("at://{}/app.bsky.feed.post/{}", row.repo, row.post_path),
                        reason: None,
                    };
                    last_dt = row.indexed_dt;
                    post.into()
                }).collect::<Vec<_>>();
                let cursor = if limit > feed.len() as u32 || last_dt == start_cursor.1 {
                    None
                } else {
                    Some(format!("{}_{}", start_cursor.0, last_dt))
                };
                Ok(get_feed_skeleton::OutputData {
                    cursor,
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
