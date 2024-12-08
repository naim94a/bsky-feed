use std::{
    borrow::Cow, collections::HashMap, future::Future, io::Cursor, str::FromStr, sync::Arc,
    time::Duration,
};

use atrium_api::{
    com::atproto::sync::subscribe_repos::{Commit, Message},
    record::KnownRecord,
};
use bitflags::bitflags;
use flume::{Receiver, Sender};
use futures::StreamExt;
use hyper::{
    header::{CONNECTION, SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_VERSION, UPGRADE, USER_AGENT},
    StatusCode,
};
use serde::Deserialize;
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

bitflags! {
    #[derive(Debug, PartialEq, Eq, Clone, Copy)]
        pub struct MessageTypes: u32 {
            const Commit = 1 << 0;
            const Identity = 1 << 1;
            const Account = 1 << 2;
            const Handle = 1 << 3;
            const Migrate = 1 << 4;
            const Tombstone = 1 << 5;
            const Info = 1 << 6;
        }

        #[derive(Debug, PartialEq, Eq, Clone, Copy)]
        pub struct Collections: u32 {
            const AppBskyActorProfile = 1 << 0;
            const AppBskyFeedGenerator = 1 << 1;
            const AppBskyFeedLike = 1 << 2;
            const AppBskyFeedPost = 1 << 3;
            const AppBskyFeedPostgate = 1 << 4;
            const AppBskyFeedRepost = 1 << 5;
            const AppBskyFeedThreadgate = 1 << 6;
            const AppBskyGraphBlock = 1 << 7;
            const AppBskyGraphFollow = 1 << 8;
            const AppBskyGraphList = 1 << 9;
            const AppBskyGraphListblock = 1 << 10;
            const AppBskyGraphListitem = 1 << 11;
            const AppBskyGraphStarterpack = 1 << 12;
            const AppBskyLabelerService = 1 << 13;
            const ChatBskyActorDeclaration = 1 << 14;
        }
}

impl FromStr for MessageTypes {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "#commit" => MessageTypes::Commit,
            "#identity" => MessageTypes::Identity,
            "#account" => MessageTypes::Account,
            "#handle" => MessageTypes::Handle,
            "#migrate" => MessageTypes::Migrate,
            "#tombstone" => MessageTypes::Tombstone,
            "#info" => MessageTypes::Info,
            _ => {
                error!("Unknown message type from firehose: '{s}'");
                return Err(());
            }
        })
    }
}

impl FromStr for Collections {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "app.bsky.actor.profile" => Collections::AppBskyActorProfile,
            "app.bsky.feed.generator" => Collections::AppBskyFeedGenerator,
            "app.bsky.feed.like" => Collections::AppBskyFeedLike,
            "app.bsky.feed.post" => Collections::AppBskyFeedPost,
            "app.bsky.feed.postgate" => Collections::AppBskyFeedPostgate,
            "app.bsky.feed.repost" => Collections::AppBskyFeedRepost,
            "app.bsky.feed.threadgate" => Collections::AppBskyFeedThreadgate,
            "app.bsky.graph.block" => Collections::AppBskyGraphBlock,
            "app.bsky.graph.follow" => Collections::AppBskyGraphFollow,
            "app.bsky.graph.list" => Collections::AppBskyGraphList,
            "app.bsky.graph.listblock" => Collections::AppBskyGraphListblock,
            "app.bsky.graph.listitem" => Collections::AppBskyGraphListitem,
            "app.bsky.graph.starterpack" => Collections::AppBskyGraphStarterpack,
            "app.bsky.labeler.service" => Collections::AppBskyLabelerService,
            "chat.bsky.actor.declaration" => Collections::ChatBskyActorDeclaration,
            _ => return Err(()),
        })
    }
}

#[derive(Debug)]
pub struct CommitOperation<T> {
    pub repo: Arc<str>,
    pub path: String,
    pub collection: Collections,
    pub cid: Option<cid::Cid>,
    pub record: T,
}

#[derive(Debug)]
pub enum RepoCommitOp {
    Create(CommitOperation<KnownRecord>),
    Update(CommitOperation<KnownRecord>),
    Delete(CommitOperation<()>),
}

pub trait FirehoseHandler {
    /// Get the last saved cursor. This allows us to resume consuming the firehose where last left off.
    fn get_last_cursor(&self) -> impl Future<Output = Option<i64>> + Send {
        async { None }
    }

    /// This is called each time an event is processed. The method should return immediately.
    /// ### Notes
    /// It is possible for this method to be called with an older cursor, this can be caused due to other threads processing faster.
    /// You should check that cursor is higher before updating. Be aware - the cursor can also reset by the firehose.
    fn update_cursor(&self, cursor: i64) -> impl Future<Output = ()> + Send {
        let _ = cursor;
        async {}
    }

    fn on_repo_operation(&self, op: RepoCommitOp) -> impl Future<Output = ()> + Send {
        let _ = op;
        async {}
    }
}

pub struct Firehose<H: Send + Sync + 'static> {
    handler: Arc<H>,
    domain: String,

    firehose_rx: Receiver<Vec<u8>>,
    firehose_tx: Sender<Vec<u8>>,

    accepted_message_types: MessageTypes,
    collections: Collections,
}

#[derive(Debug, Deserialize)]
struct Header<'a> {
    #[serde(rename(deserialize = "t"))]
    pub _type: Cow<'a, str>,
    #[serde(rename(deserialize = "op"))]
    pub _operation: u8,
}

impl<H: FirehoseHandler + Send + Sync + 'static> Firehose<H> {
    pub fn new<A: AsRef<str>>(firehose_domain: A, handler: Arc<H>) -> Self {
        let (firehose_tx, firehose_rx) = flume::bounded::<Vec<u8>>(10_000);

        Self {
            domain: firehose_domain.as_ref().to_owned(),
            handler,
            firehose_rx,
            firehose_tx,
            accepted_message_types: MessageTypes::Commit,
            collections: Collections::AppBskyFeedPost,
        }
    }

    /// Set the type of messages to process. By default only commit messages are processed.
    pub fn accept_message_types(mut self, message_types: MessageTypes) -> Self {
        self.accepted_message_types = message_types;
        self
    }

    /// Get the messages queued in
    pub fn get_queue_pending(&self) -> usize {
        self.firehose_rx.len()
    }

    /// If commit messages are processed, this allows to filter known collection types.
    /// By default only `AppBskyFeedPost` are filtered.
    pub fn accept_collections(mut self, collections: Collections) -> Self {
        self.collections = collections;
        self
    }

    #[tracing::instrument(skip_all)]
    async fn connect_firehose(&self) -> Option<fastwebsockets::WebSocket<reqwest::Upgraded>> {
        let domain = self.domain.as_str();
        let cursor = self
            .handler
            .get_last_cursor()
            .await
            .map_or(String::default(), |v| format!("?cursor={v}"));

        let mut req = reqwest::Request::new(
            reqwest::Method::GET,
            format!("https://{domain}/xrpc/com.atproto.sync.subscribeRepos{cursor}")
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
                "atproto-feedgen/",
                env!("CARGO_PKG_VERSION"),
                " (https://github.com/naim94a/bsky-feed)"
            )),
        );
        req.headers_mut().append(
            CONNECTION,
            reqwest::header::HeaderValue::from_static("upgrade"),
        );
        req.headers_mut().append(
            SEC_WEBSOCKET_KEY,
            reqwest::header::HeaderValue::from_str(&fastwebsockets::handshake::generate_key())
                .unwrap(),
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

    /// This method connects to the firehose endpoint and collects events, it should only be called once.
    /// The primary purpose of this method is to manage the network IO.
    /// It will automatically re-connect to the firehose endpoint if the connection times out.
    #[tracing::instrument(skip_all)]
    pub async fn collect_firehose_events(&self) {
        'ws: loop {
            info!("connecting to firehose endpoint...");
            let firehose = match self.connect_firehose().await {
                Some(v) => v,
                None => {
                    error!("connection failed. retrying in 5s...");
                    tokio::time::sleep(Duration::from_secs(5)).await;
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
                        if let Err(_e) = self.firehose_tx.send_async(payload).await {
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

    /// This method parses blobs read from the firehouse, and calls their handlers.
    /// Call this method for each CPU you'd like to process events.
    pub fn process_firehose_event(
        self: Arc<Self>,
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        async move {
            while let Ok(blob) = self.firehose_rx.recv_async().await {
                let (_header, message) = match self.read_subscribe_repos(&blob) {
                    None => continue,
                    Some(v) => v,
                };

                let cursor;

                match message {
                    Message::Commit(commit) => {
                        cursor = commit.seq;
                        self.process_commit(*commit).await;
                    }
                    Message::Account(v) => {
                        cursor = v.seq;
                    }
                    Message::Handle(v) => {
                        cursor = v.seq;
                    }
                    Message::Identity(identity) => {
                        cursor = identity.seq;
                    }
                    Message::Migrate(migrate) => {
                        cursor = migrate.seq;
                    }
                    Message::Tombstone(tombstone) => {
                        cursor = tombstone.seq;
                    }
                    Message::Info(info) => {
                        info!("message from firehose: '{info:?}'");
                        continue;
                    }
                };

                self.handler.update_cursor(cursor).await;
            }
        }
    }

    async fn parse_commit_blocks(data: &[u8]) -> Option<HashMap<cid::Cid, Vec<u8>>> {
        let mut res = HashMap::new();
        let mut buffer = futures::io::Cursor::new(data);
        let mut car_reader = rs_car::CarReader::new(&mut buffer, false).await.ok()?;
        while let Some(record) = car_reader.next().await {
            let (record_cid, record_data) = match record {
                Ok(v) => v,
                Err(_) => {
                    // something failed here?
                    continue;
                }
            };
            let cid = {
                let mut record_cid_bytes = [0u8; 256];
                let mut record_cid_bytes = Cursor::new(record_cid_bytes.as_mut());
                let bytes_written = match record_cid.write_bytes(&mut record_cid_bytes) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let record_cid_bytes = &record_cid_bytes.into_inner()[..bytes_written];
                match cid::Cid::read_bytes(Cursor::new(record_cid_bytes)) {
                    Ok(v) => v,
                    Err(_) => continue,
                }
            };
            res.insert(cid, record_data);
        }
        Some(res)
    }

    async fn process_commit(&self, mut commit: Commit) {
        let mut blocks = {
            let blocks = std::mem::take(&mut commit.blocks);
            Self::parse_commit_blocks(&blocks)
                .await
                .unwrap_or(HashMap::new())
        };

        let repo = commit.repo.as_str().to_owned().into_boxed_str();
        let repo: Arc<str> = Arc::from(repo);

        let ops = std::mem::take(&mut commit.ops);

        for mut op in ops.into_iter() {
            let collection = match op.path.split('/').next() {
                None => continue, // this path is malformed.
                Some(v) => match Collections::from_str(v) {
                    Ok(v) => v,
                    Err(_) => {
                        debug!("unknown collection '{v}'");
                        continue;
                    } // this collection is unknown to us
                },
            };
            if !self.collections.contains(collection) {
                continue; // collection is ignored.
            }

            let path = std::mem::take(&mut op.path);
            let cid = std::mem::take(&mut op.cid).map(|v| v.0);

            let mut parse_record = || {
                cid.map(|k| {
                    blocks
                        .remove(&k)
                        .map(|record_data| {
                            serde_cbor::from_slice::<KnownRecord>(record_data.as_ref()).ok()
                        })
                        .flatten()
                })
                .flatten()
            };

            let repo = repo.clone();

            let repo_op = match op.action.as_str() {
                "create" | "update" => {
                    let record = match parse_record() {
                        Some(v) => v,
                        None => continue,
                    };
                    let co = CommitOperation {
                        repo,
                        path,
                        collection,
                        cid,
                        record,
                    };
                    if op.action == "create" {
                        RepoCommitOp::Create(co)
                    } else if op.action == "update" {
                        RepoCommitOp::Update(co)
                    } else {
                        unreachable!()
                    }
                }
                "delete" => RepoCommitOp::Delete(CommitOperation {
                    repo,
                    path,
                    collection,
                    cid,
                    record: (),
                }),
                a => {
                    warn!("unknown repo action '{a}'");
                    continue;
                }
            };

            self.handler.on_repo_operation(repo_op).await;
        }
    }

    fn read_subscribe_repos(&self, buffer: &[u8]) -> Option<(Header, Message)> {
        let mut reader = Cursor::new(buffer);
        let header = ciborium::de::from_reader::<Header, _>(&mut reader).ok()?;
        let message_type = header._type.as_ref();

        // ignore types we have no intention to use...
        let message_type = MessageTypes::from_str(message_type).ok()?;

        if !self.accepted_message_types.contains(message_type) {
            return None;
        }

        let m = match message_type {
            MessageTypes::Commit => {
                Message::Commit(serde_ipld_dagcbor::from_reader(&mut reader).ok()?)
            }
            MessageTypes::Identity => {
                Message::Identity(serde_ipld_dagcbor::from_reader(&mut reader).ok()?)
            }
            MessageTypes::Account => {
                Message::Account(serde_ipld_dagcbor::from_reader(&mut reader).ok()?)
            }
            MessageTypes::Handle => {
                Message::Handle(serde_ipld_dagcbor::from_reader(&mut reader).ok()?)
            }
            MessageTypes::Migrate => {
                Message::Migrate(serde_ipld_dagcbor::from_reader(&mut reader).ok()?)
            }
            MessageTypes::Tombstone => {
                Message::Tombstone(serde_ipld_dagcbor::from_reader(&mut reader).ok()?)
            }
            MessageTypes::Info => Message::Info(serde_ipld_dagcbor::from_reader(&mut reader).ok()?),
            _ => unreachable!("unknown message type"),
        };
        Some((header, m))
    }
}
