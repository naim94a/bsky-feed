use std::{
    sync::{Arc, OnceLock},
    usize,
};

use atrium_api::app::bsky::feed::post::RecordData as Post;
use cid::Cid;
use lingua::Language;
use sqlx::Executor;
use tracing::{debug, error, info};

use crate::db::State;

fn is_possibly_hebrew(text: &str) -> bool {
    let hebrew_chars = 'א'..='ת';
    text.chars().any(|v| hebrew_chars.contains(&v))
}

fn get_language(txt: &str) -> Option<lingua::Language> {
    static DETECTOR: OnceLock<lingua::LanguageDetector> = OnceLock::new();
    let detector = DETECTOR
        .get_or_init(|| lingua::LanguageDetectorBuilder::from_all_spoken_languages().build());
    detector.detect_language_of(txt)
}

async fn should_add_post(db: &State, at_uri: &str, post: &mut Post) -> bool {
    if let Some(ref reply) = post.reply {
        let parent_uri = reply.parent.uri.as_str().strip_prefix("at://");
        let root_uri = reply.root.uri.as_str().strip_prefix("at://");

        match sqlx::query!(
            r#"SELECT count(*)>0 as "has_anscestors: bool" FROM post WHERE uri = ? OR uri = ?"#,
            parent_uri,
            root_uri
        )
        .fetch_optional(&db.db)
        .await
        {
            Ok(Some(v)) if v.has_anscestors => return true,
            Ok(_) => {}
            Err(e) => {
                error!("failed to check if post has existing ancestors: {e}");
            }
        }
    }

    if !is_possibly_hebrew(&post.text) {
        return false;
    }

    if let Some(ref langs) = post.langs {
        let mut has_he = false;
        let mut has_yi = false;
        for lang in langs {
            match lang.as_ref().as_str() {
                "he" => has_he = true,
                "yi" => has_yi = true,
                _ => continue,
            }
        }
        if !has_he && has_yi {
            return false;
        }
    }

    if let Some(ref facets) = post.facets {
        let mut ranges = facets
            .iter()
            .map(|f| &f.data.index)
            .map(|idx| (idx.byte_start, idx.byte_end))
            .filter(|r| r.0 < r.1 && r.1 <= post.text.len())
            .map(|(s, e)| s..e)
            .collect::<Vec<_>>();

        if !ranges.is_empty() {
            ranges.sort_by(|a, b| a.start.cmp(&b.start));

            let mut last_start = usize::MAX;
            let mut txt = post.text.clone();
            for range in ranges.into_iter().rev() {
                if range.end < last_start {
                    txt.replace_range(range.clone(), " ");
                }
                last_start = range.start;
            }
            std::mem::swap(&mut post.text, &mut txt);
        }
    }

    let post = Arc::new(post);
    let post_ = post.clone();
    let res = tokio::task::block_in_place(move || get_language(&post_.text));
    if let Some(lang) = res {
        if lang != Language::Hebrew {
            return false;
        }
    } else {
        debug!("failed to detect language of post {}", &at_uri);
        return false;
    }

    true
}

pub async fn process_post(db: &State, at_uri: String, cid: &Cid, mut post: Post) {
    if !should_add_post(db, at_uri.as_str(), &mut post).await {
        return;
    }

    info!(
        "new post: {}: {:?} - {}",
        &at_uri, &post.created_at, post.text
    );

    let cid = cid.to_string();
    let reply_root = post.reply.as_ref().map(|v| v.root.uri.as_str());
    let reply_to = post.reply.as_ref().map(|v| v.parent.uri.as_str());
    let indexed_at = std::time::UNIX_EPOCH.elapsed().unwrap().as_secs() as i64;
    let created_at = post.created_at.as_ref().timestamp();
    if let Err(e) = db
        .db
        .execute(sqlx::query!(
            "INSERT INTO post VALUES (?, ?, ?, ?, ?, ?, ?)",
            at_uri,
            cid,
            reply_root,
            reply_to,
            "he",
            indexed_at,
            created_at
        ))
        .await
    {
        error!("failed to execute query: {}", e);
    }
}

pub async fn delete_post(db: &State, at_uri: String) {
    match db
        .db
        .execute(sqlx::query!("DELETE FROM post WHERE uri = ?", at_uri))
        .await
    {
        Err(e) => tracing::error!("failed to delete post from db: {}", e),
        Ok(v) => {
            if v.rows_affected() != 0 {
                info!("deleted {}", &at_uri);
            }
        }
    }
}
