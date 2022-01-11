use thiserror::Error;

use crate::split::ProportionSplit;

/// Error type in ttv.
#[derive(Debug, Error)]
pub enum Error {
    #[error("empty file")]
    EmptyFile,
    #[error("invalid split specification: {0}")]
    InvalidSplitSpecification(String),
    #[error("invalid splits: {0:?}")]
    InvalidSplits(Vec<ProportionSplit>),

    #[error("proportion too low: {0}")]
    ProportionTooLow(String),
    #[error("proportion too high: {0}")]
    ProportionTooHigh(String),

    #[error("error parsing CSV: {0}")]
    CsvError(csv::Error),
    #[error("I/O error: {0}")]
    IoError(std::io::Error),
    #[error("error parsing float: {0}")]
    ParseFloatError(std::num::ParseFloatError),
    #[error("error parsing int: {0}")]
    ParseIntError(std::num::ParseIntError),
    #[error("internal error: {0}")]
    SendError(std::sync::mpsc::SendError<String>),
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<std::num::ParseFloatError> for Error {
    fn from(error: std::num::ParseFloatError) -> Self {
        Error::ParseFloatError(error)
    }
}

impl From<std::num::ParseIntError> for Error {
    fn from(error: std::num::ParseIntError) -> Self {
        Error::ParseIntError(error)
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Error::IoError(error)
    }
}

impl From<csv::Error> for Error {
    fn from(error: csv::Error) -> Self {
        Error::CsvError(error)
    }
}

impl From<std::sync::mpsc::SendError<String>> for Error {
    fn from(error: std::sync::mpsc::SendError<String>) -> Self {
        Error::SendError(error)
    }
}
