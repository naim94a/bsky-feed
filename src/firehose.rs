use std::{io::Cursor, sync::atomic::AtomicI64, time::Duration};

use atrium_api::com::atproto::sync::subscribe_repos::Message;
use flume::Sender;
use hyper::header::{SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_VERSION, USER_AGENT};
use reqwest::header::{CONNECTION, UPGRADE};
use serde::Deserialize;
use tracing::{debug, error, info, warn};

use crate::{db::State, feed};

#[tracing::instrument(skip_all)]
async fn connect_firehose(
    cursor: Option<i64>,
) -> Option<fastwebsockets::WebSocket<reqwest::Upgraded>> {
    let mut req = reqwest::Request::new(
        reqwest::Method::GET,
        format!(
            "https://bsky.network/xrpc/com.atproto.sync.subscribeRepos{}",
            cursor.map_or("".to_owned(), |v| format!("?cursor={v}"))
        )
        .as_str()
        .try_into()
        .ok()?,
    );
    req.headers_mut().append(
        UPGRADE,
        reqwest::header::HeaderValue::from_static("websocket"),
    );
    req.headers_mut().append(
        USER_AGENT,
        reqwest::header::HeaderValue::from_static(concat!(
            "hebrewsky-feed/",
            env!("CARGO_PKG_VERSION")
        )),
    );
    req.headers_mut().append(
        CONNECTION,
        reqwest::header::HeaderValue::from_static("upgrade"),
    );
    req.headers_mut().append(
        SEC_WEBSOCKET_KEY,
        reqwest::header::HeaderValue::from_str(&fastwebsockets::handshake::generate_key()).unwrap(),
    );
    req.headers_mut().append(
        SEC_WEBSOCKET_VERSION,
        reqwest::header::HeaderValue::from_static("13"),
    );
    let client = reqwest::ClientBuilder::default()
        // WebSocket doesn't seem to work on 2+
        .http1_only()
        .https_only(true)
        .build()
        .ok()?;

    let response = client.execute(req).await.ok()?.error_for_status().ok()?;

    assert_eq!(
        response.status(),
        reqwest::StatusCode::SWITCHING_PROTOCOLS,
        "unexpected status code from firehose endpoint"
    );

    let upgraded = response.upgrade().await.unwrap();
    Some(fastwebsockets::WebSocket::after_handshake(
        upgraded,
        fastwebsockets::Role::Client,
    ))
}

#[tracing::instrument(skip_all)]
pub async fn collect_firehose_events(tx: Sender<Vec<u8>>, cursor: Option<i64>) {
    'ws: loop {
        info!("connecting to firehose endpoint...");
        let firehose = match connect_firehose(cursor).await {
            Some(v) => v,
            None => {
                error!("connection failed. retrying in 30s...");
                tokio::time::sleep(Duration::from_secs(30)).await;
                continue 'ws;
            }
        };
        info!("firehose connected.");
        let mut firehose = fastwebsockets::FragmentCollector::new(firehose);

        loop {
            match firehose.read_frame().await {
                Ok(frame) => {
                    if frame.opcode != fastwebsockets::OpCode::Binary {
                        warn!("unexpected frame {:?}", frame.opcode);
                        continue;
                    }
                    let payload = frame.payload.to_owned();
                    if let Err(_e) = tx.send_async(payload).await {
                        // we're shutting down...
                        return;
                    }
                }
                Err(e) => {
                    error!("websocket error! {e}");
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    continue 'ws;
                }
            }
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct Header {
    #[serde(rename(deserialize = "t"))]
    pub type_: String,
    #[serde(rename(deserialize = "op"))]
    pub _operation: u8,
}

// The purpose of using this is to quickly fail if any other type exists.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "$type")]
enum KnownRecordSubset {
    #[serde(rename = "app.bsky.feed.post")]
    AppBskyFeedPost(Box<atrium_api::app::bsky::feed::post::Record>),
}

fn read_subscribe_repos(buffer: &[u8]) -> Option<(Header, Message)> {
    let mut reader = Cursor::new(buffer);
    let header = ciborium::de::from_reader::<Header, _>(&mut reader).ok()?;
    let m = match header.type_.as_str() {
        "#commit" => Message::Commit(serde_ipld_dagcbor::from_reader(&mut reader).ok()?),
        // "#handle" => Message::Handle(serde_ipld_dagcbor::from_reader(&mut reader).ok()?),
        // "#tombstone" => Message::Tombstone(serde_ipld_dagcbor::from_reader(&mut reader).ok()?),
        // "#account" => Message::Account(serde_ipld_dagcbor::from_reader(&mut reader).ok()?),
        // "#identity" => Message::Identity(serde_ipld_dagcbor::from_reader(&mut reader).ok()?),
        _ => return None,
    };
    Some((header, m))
}

#[tracing::instrument(skip_all)]
pub async fn process_firehose_blob(db: State, blob: Vec<u8>) -> Option<i64> {
    let (_header, body) = match read_subscribe_repos(&blob) {
        None => {
            // trace!("failed to parse subscribe_repo message");
            return None;
        }
        Some(v) => v,
    };

    match body {
        Message::Commit(commit) => {
            let commits = commit
                .ops
                .iter()
                .filter(|v| v.path.starts_with("app.bsky.feed.post/"));
            for op in commits {
                let get_uri = || format!("{}/{}", &commit.repo.as_str(), &op.path);
                match op.action.as_str() {
                    "create" => {
                        if let Some(ref cid) = op.cid {
                            let mut cursor = Cursor::new(&commit.blocks);
                            let _record_header = match crate::car::read_header(&mut cursor) {
                                Ok(v) => v,
                                Err(e) => {
                                    debug!("failed to parse record header: {e:?}");
                                    continue;
                                }
                            };
                            let record = match crate::car::read_blocks(&mut cursor) {
                                Ok(v) => v,
                                Err(e) => {
                                    debug!("failed to parse record body: {e:?}");
                                    continue;
                                }
                            };

                            if let Some(v) = record.get(&cid.0) {
                                match serde_cbor::from_slice::<KnownRecordSubset>(&v) {
                                    Ok(KnownRecordSubset::AppBskyFeedPost(post)) => {
                                        feed::process_post(&db, get_uri(), &cid.0, post.data).await;
                                    }
                                    Err(_e) => continue,
                                }
                            }
                        }
                    }
                    "delete" => {
                        feed::delete_post(&db, get_uri()).await;
                    }
                    _ => {}
                }
            }

            return Some(commit.seq);
        }
        _ => {
            tracing::debug!("non-commit event received.");
        }
    }
    None
}
