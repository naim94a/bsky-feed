CREATE TABLE interactions (
    /* entity that liked the post */
    repo VARCHAR(260) NOT NULL,
    path VARCHAR(260) NOT NULL,
    /* target post */
    target_repo VARCHAR(260) NOT NULL,
    target_path VARCHAR(260) NOT NULL,
    /* interaction types: like, repost */
    interaction_type VARCHAR(20) NOT NULL,
    created_at TIMESTAMP DEFAULT NULL,
    indexed_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX interactions_pair_idx ON interactions (target_repo, target_path);

CREATE UNIQUE INDEX interactions_idx ON interactions (
    repo,
    path,
    target_repo,
    target_path,
    interaction_type
);
