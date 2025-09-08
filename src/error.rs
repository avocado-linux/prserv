use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Server error: {0}")]
    Server(#[from] crate::server::ServerError),

    #[error("Database error: {0}")]
    Database(#[from] crate::database::DatabaseError),

    #[error("Client error: {0}")]
    Client(#[from] crate::client::ClientError),

    #[error("{0}")]
    Generic(String),

    #[error("{0}")]
    Other(String),
}
