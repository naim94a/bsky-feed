-- Add migration script here
CREATE TABLE post (
    uri VARCHAR(260) UNIQUE NOT NULL,
    cid VARCHAR(260) NOT NULL,
    reply_root VARCHAR(260),
    reply_to VARCHAR(260),
    language VARCHAR(2) NOT NULL DEFAULT 'iw',
    indexed_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE subscriber_state (
    service VARCHAR(260) UNIQUE NOT NULL,
    cursor INTEGER NOT NULL
);
