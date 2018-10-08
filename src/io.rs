use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::path::Path;

use flate2::read::GzDecoder;
use flate2::write::GzEncoder;

use crate::error::Result;

pub type InputReader = BufReader<Box<Read>>;
pub type OutputWriter = Box<Write>;

pub fn open_data<P: AsRef<Path>>(path: P) -> Result<InputReader> {
    // Read from stdin if input is '-', else try to open the provided file.
    let reader: Box<Read> = match path.as_ref().to_str() {
        Some(p) if p == "-" => {
            Box::new(std::io::stdin())
        },
        Some(p) => Box::new(File::open(p)?),
        _ => unreachable!(),
    };

    let gz = GzDecoder::new(reader);
    let is_compressed = gz.header().is_some();
    let final_reader: Box<Read> = if is_compressed { Box::new(gz) } else { Box::new(gz.into_inner()) };
    Ok(BufReader::with_capacity(1024 * 1024, final_reader))
}

pub fn open_output<P: AsRef<Path>>(path: P, compressed: bool) -> Result<OutputWriter> {
    let file = File::create(path)?;
    let writer: OutputWriter = if compressed {
        Box::new(GzEncoder::new(file, Default::default()))
    } else {
        Box::new(file)
    };
    Ok(writer)
}
