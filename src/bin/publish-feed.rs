use atrium_api::types::{
    string::{AtIdentifier, Did, Handle},
    Collection,
};

#[tokio::main]
async fn main() {
    dotenvy::dotenv().expect(".env required");
    let hostname = std::env::var("FEEDGEN_HOSTNAME").unwrap();
    let owner_handle = std::env::var("OWNER_HANDLE").unwrap();
    let owner_access_token = std::env::var("OWNER_TOKEN").unwrap();
    let did = Did::new(format!("did:web:{hostname}")).unwrap();

    let feed = atrium_api::app::bsky::feed::generator::RecordData {
        accepts_interactions: Some(false),
        avatar: None,
        created_at: atrium_api::types::string::Datetime::now(),
        description: Some("github.com/naim94a/bsky-feed".to_owned()),
        description_facets: None,
        did,
        display_name: "עוד פידעברית".to_owned(),
        labels: None,
    };
    let ipld = ipld_core::serde::to_ipld(&feed).unwrap();

    let op = atrium_api::com::atproto::repo::put_record::InputData {
        collection: atrium_api::app::bsky::feed::Generator::nsid(),
        rkey: "hebrew-feed".to_owned(),
        repo: AtIdentifier::Handle(Handle::new(owner_handle.clone()).unwrap()),
        swap_commit: None,
        swap_record: None,
        validate: Some(true),
        record: atrium_api::types::Unknown::Other(ipld.try_into().unwrap()),
    };

    let client = bsky_sdk::BskyAgent::builder().build().await.unwrap();
    client
        .login(owner_handle, owner_access_token)
        .await
        .unwrap();
    client
        .api
        .com
        .atproto
        .repo
        .put_record(op.into())
        .await
        .unwrap();
}
