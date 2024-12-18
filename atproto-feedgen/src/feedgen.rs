use serde::Serialize;
use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};

use atrium_api::{
    app::bsky::feed::{describe_feed_generator, get_feed_skeleton},
    types::string::Did,
};
use bytes::Bytes;

pub type FetchPostCallbackRes = Pin<
    Box<
        dyn Future<Output = Result<get_feed_skeleton::OutputData, get_feed_skeleton::Error>> + Send,
    >,
>;
pub type FetchPostsCallback<S> =
    dyn Fn(S, Option<Did>, u32, Option<String>) -> FetchPostCallbackRes + Send + Sync;

struct Feed<S> {
    gen_callback: Box<FetchPostsCallback<S>>,
    auth_required: bool,
}

pub struct FeedManager<S: Clone> {
    state: S,
    /// feed did - usually did:web:<hostname>
    feed_did: Arc<Did>,
    /// owner did - did of the account managing the feed, did:plc:<pubkey>.
    owner_did: Did,
    fetch_posts: HashMap<String, Feed<S>>,

    // re-usable responses
    description: Bytes,
    feed_prefix: String,
    well_known: Bytes,
}

impl<St: Clone + Send + Sync> FeedManager<St> {
    pub fn new(feed_did: Did, owner_did: Did, domain: &str, state: St) -> Self {
        let feed_prefix = format!("at://{}/app.bsky.feed.generator/", owner_did.as_str());
        let mut r = Self {
            state,
            feed_did: Arc::new(feed_did),
            owner_did,
            description: Bytes::new(),
            fetch_posts: Default::default(),
            feed_prefix,
            well_known: Bytes::new(),
        };
        r.set_description();
        r.set_well_known(domain);
        r
    }

    fn set_well_known(&mut self, domain: &str) {
        let svc = WellKnownDidService {
            id: "#bsky_fg",
            type_: "BskyFeedGenerator",
            service_endpoint: &format!("https://{domain}"),
        };
        let did = WellKnownDid {
            context_: &["https://www.w3.org/ns/did/v1"],
            id: &self.get_did(),
            service: &[svc],
        };
        self.well_known = Bytes::from(serde_json::to_string(&did).unwrap());
    }

    pub fn register_feed<S: Into<String>, F>(&mut self, name: S, callback: F, auth_required: bool)
    where
        F: Fn(St, Option<Did>, u32, Option<String>) -> FetchPostCallbackRes + Send + Sync + 'static,
    {
        self.fetch_posts.insert(
            name.into(),
            Feed {
                gen_callback: Box::new(callback),
                auth_required,
            },
        );
        self.set_description();
    }

    pub fn is_user_required(&self, feed_name: &str) -> bool {
        match self.fetch_posts.get(feed_name) {
            Some(f) => f.auth_required,
            _ => false,
        }
    }

    pub fn fetch_posts(
        &self,
        feed_name: &str,
        user: Option<Did>,
        limit: u32,
        cursor: Option<String>,
    ) -> FetchPostCallbackRes {
        let feed = match self.fetch_posts.get(feed_name) {
            Some(v) => v.gen_callback.as_ref(),
            None => {
                return Box::pin(async { Err(get_feed_skeleton::Error::UnknownFeed(None)) });
            }
        };
        feed(self.state.clone(), user, limit, cursor)
    }

    fn set_description(&mut self) {
        let feeds: Vec<_> = self
            .fetch_posts
            .keys()
            .map(|feed_name| {
                describe_feed_generator::FeedData {
                    uri: format!(
                        "at://{}/app.bsky.feed.generator/{feed_name}",
                        self.owner_did.as_str()
                    ),
                }
                .into()
            })
            .collect();
        let output = describe_feed_generator::OutputData {
            did: (*self.feed_did).clone(),
            feeds,
            links: None,
        };
        self.description = Bytes::from_owner(serde_json::to_string(&output).unwrap())
    }

    pub fn get_did(&self) -> Arc<Did> {
        self.feed_did.clone()
    }

    pub fn get_feed_prefix(&self) -> &str {
        &self.feed_prefix
    }

    pub fn describe(&self) -> Bytes {
        self.description.clone()
    }

    pub fn well_known_did(&self) -> Bytes {
        self.well_known.clone()
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WellKnownDidService<'a> {
    id: &'a str,
    #[serde(rename = "type")]
    type_: &'a str,
    service_endpoint: &'a str,
}

#[derive(Serialize)]
struct WellKnownDid<'a> {
    #[serde(rename = "@context")]
    context_: &'a [&'a str],
    id: &'a str,
    service: &'a [WellKnownDidService<'a>],
}
