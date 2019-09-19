use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::path::Path;

use flate2::read::GzDecoder;
use flate2::write::GzEncoder;

use crate::error::Result;

pub type InputReader = BufReader<Box<dyn Read>>;
pub type OutputWriter = Box<dyn Write>;

#[derive(Clone, Copy, Debug)]
pub enum Compression {
    Uncompressed,
    GzipCompression,
}

pub fn open_data<P: AsRef<Path>>(path: P, compression: Compression) -> Result<InputReader> {
    // Read from stdin if input is '-', else try to open the provided file.
    let reader: Box<dyn Read> = match path.as_ref().to_str() {
        Some(p) if p == "-" => Box::new(std::io::stdin()),
        Some(p) => Box::new(File::open(p)?),
        _ => unreachable!(),
    };

    let reader: Box<dyn Read> = match compression {
        Compression::Uncompressed => Box::new(reader),
        Compression::GzipCompression => Box::new(GzDecoder::new(reader)),
    };
    Ok(BufReader::with_capacity(1024 * 1024, reader))
}

pub fn open_output<P: AsRef<Path>>(path: P, compression: Compression) -> Result<OutputWriter> {
    let file = File::create(path)?;
    let writer: OutputWriter = match compression {
        Compression::GzipCompression => Box::new(GzEncoder::new(file, Default::default())),
        Compression::Uncompressed => Box::new(file),
    };
    Ok(writer)
}
