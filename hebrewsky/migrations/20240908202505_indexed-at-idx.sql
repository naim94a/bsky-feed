-- Add migration script here
CREATE INDEX post_indexed_dt_idx ON post (indexed_dt);
