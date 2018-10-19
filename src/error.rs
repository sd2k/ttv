use std::fmt;

use crate::split::ProportionSplit;

/// Error type in ttv.
#[derive(Debug)]
pub enum Error {
    EmptyFile,
    InvalidSplitSpecification(String),
    InvalidSplits(Vec<ProportionSplit>),

    ProportionTooLow(String),
    ProportionTooHigh(String),

    IoError(std::io::Error),
    ParseFloatError(std::num::ParseFloatError),
    ParseIntError(std::num::ParseIntError),
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

impl From<std::sync::mpsc::SendError<String>> for Error {
    fn from(error: std::sync::mpsc::SendError<String>) -> Self {
        Error::SendError(error)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}
