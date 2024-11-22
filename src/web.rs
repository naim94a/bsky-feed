use std::i64;
use std::sync::OnceLock;

use crate::db::State;
use atrium_api::app::bsky::feed::describe_feed_generator;
use atrium_api::app::bsky::feed::get_feed_skeleton;
use atrium_api::types::string::Did;
use axum::Json;
use serde::{Deserialize, Serialize};
use tower_http::compression::CompressionLayer;
use tower_http::trace::TraceLayer;
use tracing::{error, trace};

use axum::async_trait;
use axum::extract::FromRequestParts;
use base64::Engine;
use hyper::header;

#[derive(Serialize)]
#[serde(untagged)]
enum GetFeedSkeletorResult {
    Ok(get_feed_skeleton::OutputData),
    Err(get_feed_skeleton::Error),
}

struct StaticRes {
    hostname: &'static str,
    did: &'static str,
    feed_prefix: &'static str,
    uri: &'static str,
}

const FEEDS: &[&str] = &["hebrew-feed"];

fn get_res() -> &'static StaticRes {
    static RES: OnceLock<StaticRes> = OnceLock::new();
    RES.get_or_init(|| {
        let hostname = String::leak(std::env::var("FEEDGEN_HOSTNAME").unwrap());
        let did = String::leak(format!("did:web:{hostname}"));
        let uri = String::leak(format!("https://{hostname}"));
        let feed_prefix = String::leak(format!(
            "at://{}/app.bsky.feed.generator/",
            std::env::var("OWNER_DID").unwrap()
        ));
        StaticRes {
            hostname,
            did,
            uri,
            feed_prefix,
        }
    })
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
    axum::extract::State(s): axum::extract::State<State>,
    axum::extract::Query(q): axum::extract::Query<get_feed_skeleton::ParametersData>,
    _auth: Option<AuthExtractor>,
) -> axum::Json<GetFeedSkeletorResult> {
    let requested_feed = q.feed.as_str();
    if !requested_feed.starts_with(get_res().feed_prefix) {
        return GetFeedSkeletorResult::Err(get_feed_skeleton::Error::UnknownFeed(None)).into();
    }
    let requested_feed = requested_feed.split_at(get_res().feed_prefix.len()).1;
    match requested_feed {
        "hebrew-feed" => {
            let mut feed = vec![];
            let limit = q.limit.map(|v| u8::from(v)).unwrap_or(100) as i32;
            let start_cursor = match q.cursor {
                None => None,
                Some(v) => match v.parse::<i64>() {
                    Ok(v) => Some(v),
                    Err(_) => None,
                },
            };
            let mut cursor = None;
            match sqlx::query!(
                r#"SELECT uri, indexed_dt as "indexed: i64" FROM post WHERE (? is NULL OR indexed_dt < ?) ORDER BY (indexed_dt/60) DESC, created_at DESC LIMIT ?"#,
                start_cursor,
                start_cursor,
                limit
            )
            .fetch_all(&s.db)
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
                                post: format!("at://{}", row.uri),
                                reason: None,
                            };
                            post.into()
                        })
                        .collect();
                }
            }
            if limit > feed.len() as i32 || cursor == start_cursor {
                cursor = None;
            }
            GetFeedSkeletorResult::Ok(get_feed_skeleton::OutputData {
                cursor: cursor.map(|v| v.to_string()),
                feed,
            })
            .into()
        }
        _ => GetFeedSkeletorResult::Err(get_feed_skeleton::Error::UnknownFeed(None)).into(),
    }
}

pub async fn describe_feed_generator() -> Json<describe_feed_generator::OutputData> {
    let feeds = FEEDS
        .iter()
        .map(|&feed| {
            describe_feed_generator::FeedData {
                uri: format!("{}{feed}", get_res().feed_prefix),
            }
            .into()
        })
        .collect();
    Json(describe_feed_generator::OutputData {
        did: Did::new(format!("did:web:{}", get_res().hostname)).unwrap(),
        feeds,
        links: None,
    })
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WellKnownDidService {
    id: &'static str,
    #[serde(rename = "type")]
    type_: &'static str,
    service_endpoint: &'static str,
}

#[derive(Serialize)]
struct WellKnownDid {
    #[serde(rename = "@context")]
    context_: &'static [&'static str],
    id: &'static str,
    service: [WellKnownDidService; 1],
}

async fn well_known_did(
    axum::extract::State(_s): axum::extract::State<State>,
) -> Json<WellKnownDid> {
    let svc = WellKnownDidService {
        id: "#bsky_fg",
        type_: "BskyFeedGenerator",
        service_endpoint: get_res().uri,
    };
    Json(WellKnownDid {
        context_: &["https://www.w3.org/ns/did/v1"],
        id: get_res().did,
        service: [svc],
    })
}

pub async fn web_server(state: State) {
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
        .with_state(state);

    axum::serve(
        tokio::net::TcpListener::bind("0.0.0.0:80").await.unwrap(),
        app,
    )
    .await
    .unwrap();
}
