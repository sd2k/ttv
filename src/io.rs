use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::path::Path;

use flate2::read::GzDecoder;
use flate2::write::GzEncoder;

use crate::error::Result;

pub type OutputWriter = Box<dyn Write>;

#[derive(Clone, Copy, Debug)]
pub enum Compression {
    Uncompressed,
    GzipCompression,
}

pub trait LineReader {
    fn read_line(&mut self) -> Option<Result<String>>;
}

impl LineReader for csv::Reader<Box<dyn Read>> {
    fn read_line(&mut self) -> Option<Result<String>> {
        let mut record = csv::ByteRecord::with_capacity(1024, 100);
        match self.read_byte_record(&mut record) {
            Ok(read) if read => {
                let curs = std::io::Cursor::new(Vec::with_capacity(1024));
                let mut writer = csv::Writer::from_writer(curs);
                writer.write_byte_record(&record).unwrap();
                let s = String::from_utf8(writer.into_inner().unwrap().into_inner()).unwrap();
                Some(Ok(s))
            }
            Ok(_) => None,
            Err(e) => Some(Err(e.into())),
        }
    }
}

impl LineReader for BufReader<Box<dyn Read>> {
    fn read_line(&mut self) -> Option<Result<String>> {
        let mut buf = String::with_capacity(1024);
        match std::io::BufRead::read_line(self, &mut buf) {
            Ok(n) if n == 0 => None,
            Ok(_) => Some(Ok(buf)),
            Err(e) => Some(Err(e.into())),
        }
    }
}

pub fn open_data<P: AsRef<Path>>(
    path: P,
    compression: Compression,
    csv_builder: Option<csv::ReaderBuilder>,
) -> Result<Box<dyn LineReader>> {
    // Read from stdin if input is '-', else try to open the provided file.
    let reader: Box<dyn Read> = match path.as_ref().to_str() {
        Some(p) if p == "-" => Box::new(std::io::stdin()),
        Some(p) => Box::new(File::open(p)?),
        _ => unreachable!(),
    };

    let reader: Box<dyn Read> = match compression {
        Compression::Uncompressed => reader,
        Compression::GzipCompression => Box::new(GzDecoder::new(reader)),
    };

    let reader: Box<dyn LineReader> = match csv_builder {
        Some(builder) => Box::new(builder.from_reader(reader)),
        None => Box::new(BufReader::with_capacity(1024 * 1024, reader)),
    };
    Ok(reader)
}

pub fn open_output<P: AsRef<Path>>(path: P, compression: Compression) -> Result<OutputWriter> {
    let file = File::create(path)?;
    let writer: OutputWriter = match compression {
        Compression::GzipCompression => Box::new(GzEncoder::new(file, Default::default())),
        Compression::Uncompressed => Box::new(file),
    };
    Ok(writer)
}
