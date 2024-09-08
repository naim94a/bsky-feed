-- Add migration script here
ALTER TABLE post
    ADD COLUMN created_at TIMESTAMP DEFAULT NULL;
