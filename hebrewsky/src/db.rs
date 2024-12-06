use sqlx::migrate::Migrator;

static MIGRATIONS: Migrator = sqlx::migrate!();

#[derive(Clone)]
pub struct State {
    pub db: sqlx::SqlitePool,
}

pub async fn new() -> State {
    let options = sqlx::sqlite::SqliteConnectOptions::new()
        .create_if_missing(true)
        .filename("db.sqlite")
        .auto_vacuum(sqlx::sqlite::SqliteAutoVacuum::Incremental)
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal);

    let db = sqlx::SqlitePool::connect_with(options).await.unwrap();

    build_db(&db).await;

    State { db }
}

async fn build_db(db: &sqlx::SqlitePool) {
    MIGRATIONS.run(db).await.expect("migrations failed.");
}
