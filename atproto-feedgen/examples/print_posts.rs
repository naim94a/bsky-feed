use std::sync::Arc;

use atproto_feedgen::{self};

struct MyFeedGenerator;
impl atproto_feedgen::FirehoseHandler for MyFeedGenerator {
    async fn get_last_cursor(&self) -> Option<i64> {
        None
    }

    async fn update_cursor(&self, cursor: i64) {
        if cursor % 10_000 == 0 {
            println!("cursor @ {cursor}");
        }
    }

    async fn on_repo_operation(&self, op: atproto_feedgen::RepoCommitOp) {
        match op {
            atproto_feedgen::RepoCommitOp::Create(commit_operation) => {}
            atproto_feedgen::RepoCommitOp::Update(commit_operation) => {}
            atproto_feedgen::RepoCommitOp::Delete(commit_operation) => {
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
