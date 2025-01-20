use std::sync::OnceLock;

use atrium_api::did_doc::DidDocument;

const PLC_DIRECTORY: &str = "https://plc.directory/";

pub fn get_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| reqwest::ClientBuilder::new().build().unwrap())
}

pub async fn resolve_did(did: &str) -> Result<DidDocument, ()> {
    if did.starts_with("did:plc:") {
        get_client()
            .get(format!("{PLC_DIRECTORY}{did}"))
            .send()
            .await
            .map_err(|_| ())?
            .json()
            .await
            .map_err(|_| ())
    } else if did.starts_with("did:web:") {
        Err(())
    } else {
        Err(())
    }
}
