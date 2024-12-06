-- Add migration script here
CREATE TABLE ignores (
    did VARCHAR(255) PRIMARY KEY,
    reason TEXT DEFAULT NULL,
    created TIMESTAMP NOT NULL
);
