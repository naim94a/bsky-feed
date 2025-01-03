CREATE TABLE post_v2 (
    /* record info */
    repo VARCHAR(260) NOT NULL,
    post_path VARCHAR(260) NOT NULL,
    cid VARCHAR(260) NOT NULL,
    /* post info */
    reply_root VARCHAR(260),
    reply_to VARCHAR(260),
    /* generated by us */
    language VARCHAR(2) NOT NULL DEFAULT 'he',
    inc_reason VARCHAR(50) DEFAULT NULL,
    is_hidden BOOLEAN DEFAULT FALSE,
    indexed_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT NULL
);

CREATE INDEX post_repo_idx ON post_v2 (repo);

CREATE UNIQUE INDEX post_path_idx ON post_v2 (repo, post_path);

CREATE UNIQUE INDEX post_cid_idx ON post_v2 (repo, cid);

CREATE INDEX post_indexed_idx ON post_v2 (indexed_dt DESC);

INSERT INTO
    post_v2
select
    substr (uri, 0, instr (uri, '/app.bsky.feed.post/')),
    substr (uri, instr (uri, '/app.bsky.feed.post/') + 20),
    cid,
    reply_root,
    reply_to,
    language,
    NULL,
    FALSE,
    indexed_dt,
    created_at
from
    post;

DROP TABLE post;

ALTER TABLE post_v2
RENAME TO post;
