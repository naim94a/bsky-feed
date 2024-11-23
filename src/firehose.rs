use std::{borrow::Cow, io::Cursor, time::Duration};

use atrium_api::com::atproto::sync::subscribe_repos::Message;
use flume::Sender;
use futures::StreamExt;
use hyper::{
    header::{SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_VERSION, USER_AGENT},
    StatusCode,
};
use reqwest::header::{CONNECTION, UPGRADE};
use serde::Deserialize;
use tokio::time::timeout;
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
        .read_timeout(Duration::from_secs(10))
        .connect_timeout(Duration::from_secs(10))
        .build()
        .ok()?;

    let response = client.execute(req).await.ok()?.error_for_status().ok()?;

    if response.status() != StatusCode::SWITCHING_PROTOCOLS {
        error!("unexpected http response: {}", response.status());
        return None;
    }

    let upgraded = response.upgrade().await.ok()?;
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
            let v = match timeout(Duration::from_secs(10), firehose.read_frame()).await {
                Ok(v) => v,
                Err(err) => {
                    warn!("read_frame timeout. {err}");
                    return;
                }
            };
            match v {
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
                    return;
                }
            }
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct Header<'a> {
    #[serde(rename(deserialize = "t"))]
    pub type_: Cow<'a, str>,
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
    let m = match header.type_.as_ref() {
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
                let uri = format!("{}/{}", &commit.repo.as_str(), &op.path);

                match op.action.as_str() {
                    "create" => {
                        if let Some(ref cid) = op.cid {
                            let mut buffer = futures::io::Cursor::new(&commit.blocks);
                            let mut car_reader =
                                match rs_car::CarReader::new(&mut buffer, false).await {
                                    Ok(v) => v,
                                    Err(e) => {
                                        debug!("failed to parse record header: {e:?}");
                                        continue;
                                    }
                                };
                            while let Some(record) = car_reader.next().await {
                                let (record_cid, record_data) = match record {
                                    Ok(v) => v,
                                    Err(_) => continue,
                                };

                                // currently using conflicting `cid` crate versions...
                                {
                                    let mut rec_cid = [0u8; 256];
                                    let mut rec_cid = Cursor::new(rec_cid.as_mut_slice());
                                    let mut exp_cid = [0u8; 256];
                                    let mut exp_cid = Cursor::new(exp_cid.as_mut_slice());

                                    let rec_cid = match record_cid.write_bytes(&mut rec_cid) {
                                        Ok(sz) => &rec_cid.into_inner()[..sz],
                                        Err(_) => continue,
                                    };
                                    let exp_cid = match cid.0.write_bytes(&mut exp_cid) {
                                        Ok(sz) => &exp_cid.into_inner()[..sz],
                                        Err(_) => continue,
                                    };

                                    if rec_cid != exp_cid {
                                        continue;
                                    }
                                }

                                match serde_cbor::from_slice::<KnownRecordSubset>(&record_data) {
                                    Ok(KnownRecordSubset::AppBskyFeedPost(post)) => {
                                        feed::process_post(&db, &uri, &cid.0, post.data).await;
                                    }
                                    Err(_e) => continue,
                                }
                            }
                        }
                    }
                    "delete" => {
                        feed::delete_post(&db, &uri).await;
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
