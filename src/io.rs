use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use flate2::read::GzDecoder;
use flate2::write::GzEncoder;

use crate::error::Result;

pub type GzReader = BufReader<GzDecoder<File>>;
pub type GzWriter = GzEncoder<File>;

pub fn open_data<P: AsRef<Path>>(path: P) -> Result<GzReader> {
    let file = File::open(path)?;
    let gz = GzDecoder::new(file);
    Ok(BufReader::with_capacity(1024 * 1024, gz))
}

pub fn open_output<P: AsRef<Path>>(path: P) -> Result<GzWriter> {
    let file = File::create(path)?;
    Ok(GzWriter::new(file, Default::default()))
}
