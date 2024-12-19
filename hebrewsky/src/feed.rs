use atproto_feedgen::Cid;
use atrium_api::app::bsky::feed::post::RecordData as Post;
use sqlx::Executor;
use std::{
    sync::{Arc, OnceLock},
    usize,
};
use tracing::{debug, error, info};
use whatlang::Lang;

use crate::AtUriParts;

fn is_possibly_hebrew(text: &str) -> bool {
    let hebrew_chars = 'א'..='ת';
    text.chars().any(|v| hebrew_chars.contains(&v))
}

fn get_language(txt: &str) -> Option<whatlang::Lang> {
    static DETECTOR: OnceLock<whatlang::Detector> = OnceLock::new();
    let detector = DETECTOR.get_or_init(|| whatlang::Detector::new());
    detector.detect_lang(txt)
}

#[tracing::instrument(skip_all)]
async fn is_ignored(db: &sqlx::SqlitePool, at_uri: &AtUriParts<'_>) -> bool {
    match sqlx::query!(
        r#"SELECT count(*)>0 as "ignored: i32" FROM ignores WHERE did = ?"#,
        at_uri.repo,
    )
    .fetch_optional(db)
    .await
    {
        Ok(Some(res)) if res.ignored != 0 => return true,
        Ok(_) => return false,
        Err(err) => {
            error!("failed to check if ignored! {err}");
            return false;
        }
    }
}

fn split_uri(uri: &str) -> Option<(&str, &str)> {
    let uri = uri.strip_prefix("at://")?;
    let mut s = uri.splitn(2, "/app.bsky.feed.post/");
    Some((s.next()?, s.next()?))
}

#[tracing::instrument(skip_all)]
async fn should_add_post(db: &sqlx::SqlitePool, at_uri: &AtUriParts<'_>, post: &mut Post) -> bool {
    if let Some(ref reply) = post.reply {
        let (parent_repo, parent_path) = split_uri(reply.parent.uri.as_str())
            .map(|v| (Some(v.0), Some(v.1)))
            .unwrap_or_default();
        let (root_repo, root_path) = split_uri(reply.root.uri.as_str())
            .map(|v| (Some(v.0), Some(v.1)))
            .unwrap_or_default();

        match sqlx::query!(
            r#"SELECT count(*)>0 as "has_anscestors: bool" FROM post WHERE (repo = ? AND post_path = ?) OR (repo = ? AND post_path = ?)"#,
            parent_repo, parent_path,
            root_repo, root_path,
        )
        .fetch_optional(db)
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

    if is_ignored(db, at_uri).await {
        debug!("DID is ignored: {at_uri}");
        return false;
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
        debug!("lang = {lang:?}");
        if lang != Lang::Heb {
            return false;
        }
    } else {
        debug!("failed to detect language of post {at_uri}",);
        return false;
    }

    true
}

#[tracing::instrument(skip_all)]
pub async fn process_post(
    db: &sqlx::SqlitePool,
    at_uri: AtUriParts<'_>,
    cid: &Cid,
    mut post: Post,
) {
    if !should_add_post(db, &at_uri, &mut post).await {
        return;
    }

    let at_repo_path = match at_uri.path.strip_prefix("app.bsky.feed.post/") {
        Some(v) => v,
        None => {
            if cfg!(debug_assertions) {
                unreachable!("non app.bsky.feed.post reached process_post");
            }
            return;
        }
    };

    info!(
        "new post: {}: {:?} - {}",
        &at_uri, &post.created_at, post.text
    );

    let cid = cid.to_string();
    let reply_root = post.reply.as_ref().map(|v| v.root.uri.as_str());
    let reply_to = post.reply.as_ref().map(|v| v.parent.uri.as_str());
    let indexed_at = std::time::UNIX_EPOCH.elapsed().unwrap().as_secs() as i64;
    let created_at = post.created_at.as_ref().timestamp();
    let content = post.text.as_str();
    if let Err(e) = db
        .execute(sqlx::query!(
            r#"INSERT INTO post (
                repo, post_path, cid,
                reply_root, reply_to,
                content,
                language,
                indexed_dt, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
            at_uri.repo,
            at_repo_path,
            cid,
            reply_root,
            reply_to,
            content,
            "he",
            indexed_at,
            created_at
        ))
        .await
    {
        error!("failed to execute query: {}", e);
    }
}

#[tracing::instrument(skip_all)]
pub async fn delete_post(db: &sqlx::SqlitePool, at_uri: AtUriParts<'_>) {
    match db
        .execute(sqlx::query!(
            "DELETE FROM post WHERE repo = ? AND post_path = ?",
            at_uri.repo,
            at_uri.path
        ))
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
