# yet another bsky feed
This is a [Bluesky](https://bsky.app/) feed generator written in Rust.

## Development
- sqlx setup
  ```bash
  cargo install sqlx-cli

  echo DATABASE_URL=sqlite://db.sqlite > .env
  cargo sqlx database create
  caego sqlx prepare
  ```
