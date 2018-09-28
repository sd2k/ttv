/// Error type in ttv.
#[derive(Debug)]
pub enum Error {
    EmptyFile,
    InvalidSplitSpecification(String),
    InvalidSplitSpecifications(String),
    IoError(std::io::Error),
    ParseFloatError(std::num::ParseFloatError),
    ParseIntError(std::num::ParseIntError),
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
