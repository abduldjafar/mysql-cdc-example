use thiserror::Error;

use crate::config::CdcEvent;

#[derive(Error, Debug)]
pub enum BinlogTapError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("MySQL error: {0}")]
    Mysql(#[from] mysql_async::Error),
    #[error("TOML parse error: {0}")]
    TomlParse(#[from] toml::de::Error),
    #[error("Tokio join error: {0}")]
    TokioJoin(#[from] tokio::task::JoinError),
    #[error("Channel send error: {0}")]
    ChannelSend(#[from] tokio::sync::mpsc::error::SendError<CdcEvent>),
}

pub type Result<T> = std::result::Result<T, BinlogTapError>;
