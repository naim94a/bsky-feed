use std::sync::Arc;

use atproto_feedgen::{self, Collections, MessageTypes};

struct MyFeedGenerator;
impl atproto_feedgen::FirehoseHandler for MyFeedGenerator {
    async fn get_last_cursor(&self) -> Option<u64> {
        None
    }

    async fn update_cursor(&self, cursor: u64) {
        if cursor % 10_000 == 0 {
            println!("cursor @ {cursor}");
        }
    }

    async fn on_repo_operation<'a>(&self, op: atproto_feedgen::RepoOp<'_>) {
        match op {
            atproto_feedgen::RepoOp::Create(commit_operation) => {}
            atproto_feedgen::RepoOp::Update(commit_operation) => {}
            atproto_feedgen::RepoOp::Delete(commit_operation) => {
                println!("{commit_operation:?}");
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let handler = Arc::new(MyFeedGenerator {});
    let firehose = atproto_feedgen::Firehose::new("bsky.network", handler);
    // let firehose = firehose.accept_collections(Collections::all());
    // let firehose = firehose.accept_message_types(!MessageTypes::Commit);
    let firehose = Arc::new(firehose);

    {
        let firehose = firehose.clone();
        tokio::spawn(async move {
            firehose.collect_firehose_events().await;
        });
    }

    firehose.process_firehose_event().await;
}
