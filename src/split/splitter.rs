use std::collections::HashMap;
use std::path::{Path, PathBuf};

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use log::{debug, info};
use rand::prelude::*;
use rand_chacha::ChaChaRng;

use crate::error::{Error, Result};
use crate::io::{open_data, Compression};
use crate::split::{
    single::{ProportionSplit, RowSplit, Split, SplitEnum},
    splits::{SplitSelection, Splits},
    writer::SplitWriter,
};

pub struct SplitterBuilder {
    /// The path to the input file
    input: PathBuf,
    /// The desired splits
    splits: Splits,
    /// The seed used for randomisation
    seed: Option<u64>,
    /// The prefix for the output file(s)
    output_prefix: Option<PathBuf>,
    /// The maximum size of each chunk
    chunk_size: Option<u64>,
    /// The total number of rows
    total_rows: Option<u64>,
    /// Compression for input files
    input_compression: Compression,
    /// Compression for output files
    output_compression: Compression,
    /// Is the input CSV?
    csv: bool,
    /// Does the input have headers?
    ///
    /// Note: defaults to true.
    has_header: bool,
}

impl SplitterBuilder {
    pub fn new<P: AsRef<Path>>(
        input: &P,
        row_splits: Vec<RowSplit>,
        prop_splits: Vec<ProportionSplit>,
    ) -> Result<Self> {
        let splits = if row_splits.is_empty() {
            Splits::Proportions(prop_splits.try_into()?)
        } else {
            Splits::Rows(row_splits.into())
        };
        Ok(SplitterBuilder {
            input: input.as_ref().to_path_buf(),
            splits,
            seed: None,
            output_prefix: None,
            chunk_size: None,
            total_rows: None,
            input_compression: Compression::Uncompressed,
            output_compression: Compression::Uncompressed,
            csv: false,
            has_header: true,
        })
    }

    #[must_use]
    pub fn seed(mut self, seed: u64) -> Self {
        self.seed = Some(seed);
        self
    }

    #[must_use]
    pub fn output_prefix(mut self, output_prefix: PathBuf) -> Self {
        self.output_prefix = Some(output_prefix);
        self
    }

    #[must_use]
    pub fn chunk_size(mut self, chunk_size: u64) -> Self {
        self.chunk_size = Some(chunk_size);
        self
    }

    #[must_use]
    pub fn total_rows(mut self, total_rows: u64) -> Self {
        self.total_rows = Some(total_rows);
        self
    }

    #[must_use]
    pub fn input_compression(mut self, input_compression: Compression) -> Self {
        self.input_compression = input_compression;
        self
    }

    #[must_use]
    pub fn output_compression(mut self, output_compression: Compression) -> Self {
        self.output_compression = output_compression;
        self
    }

    #[must_use]
    pub fn csv(mut self, csv: bool) -> Self {
        self.csv = csv;
        self
    }

    #[must_use]
    pub fn has_header(mut self, has_header: bool) -> Self {
        self.has_header = has_header;
        self
    }

    pub fn build(self) -> Result<Splitter> {
        let rng = match self.seed {
            Some(s) => ChaChaRng::seed_from_u64(s),
            None => ChaChaRng::from_entropy(),
        };
        Ok(Splitter {
            input: self.input,
            rng,
            splits: self.splits,
            output_prefix: self.output_prefix,
            chunk_size: self.chunk_size,
            total_rows: self.total_rows,
            input_compression: self.input_compression,
            output_compression: self.output_compression,
            csv: self.csv,
            has_header: self.has_header,
        })
    }
}

pub struct Splitter {
    /// The path to the input file
    input: PathBuf,
    /// The desired splits
    splits: Splits,
    /// The stateful random number generator.
    rng: ChaChaRng,
    /// The prefix for the output file(s)
    output_prefix: Option<PathBuf>,
    /// The maximum size of each chunk
    chunk_size: Option<u64>,
    /// The total number of rows
    total_rows: Option<u64>,
    /// Compression for input files
    input_compression: Compression,
    /// Compression for output files
    output_compression: Compression,
    /// Is the input CSV?
    csv: bool,
    /// Does the input have headers?
    ///
    /// Note: defaults to true.
    has_header: bool,
}

impl Splitter {
    pub fn run(mut self) -> Result<()> {
        let multi = MultiProgress::new();

        // Use a slightly different progress bar depending on the situation
        let progress: HashMap<String, ProgressBar> = match (&self.splits, self.total_rows) {
            (Splits::Proportions(p), Some(t)) => p
                .splits
                .iter()
                .map(|p| {
                    let name = p.name().to_string();
                    let style = ProgressStyle::default_bar()
                        .template("{msg:<10}: [{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/~{len:7} (ETA: {eta_precise})")
                        .expect("valid indicatif template")
                        .progress_chars("█▉▊▋▌▍▎▏  ");
                    let split_total = p.proportion * t as f64;
                    let pb = multi.add(ProgressBar::new(split_total as u64));
                    pb.set_message(name.clone());
                    pb.set_style(style);
                    (name, pb)
                })
                .collect(),
            (Splits::Proportions(p), None) => p
                .splits
                .iter()
                .map(|p| {
                    let name = p.name().to_string();
                    let style = ProgressStyle::default_bar()
                        .template("{msg:<10}: [{elapsed_precise}] {spinner:.green} {pos:>7}")
                        .expect("valid indicatif template");
                    let pb = multi.add(ProgressBar::new_spinner());
                    pb.set_style(style);
                    pb.set_message(name.clone());
                    (name, pb)
                })
                .collect(),
            (Splits::Rows(r), _) => r
                .splits
                .iter()
                .map(|r| {
                    let name = r.name().to_string();
                    let style = ProgressStyle::default_bar()
                        .template("{msg:<10}: [{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} (ETA: {eta_precise})")
                        .expect("valid indicatif template")
                        .progress_chars("█▉▊▋▌▍▎▏  ");
                    let pb = multi.add(ProgressBar::new(r.total as u64));
                    pb.set_message(name.clone());
                    pb.set_style(style);
                    (name, pb)
                })
                .collect()
        };

        let mut senders = HashMap::new();
        let mut chunk_writers = Vec::new();
        let output_path = match self.output_prefix {
            Some(ref f) => f.clone(),
            None => self.input.clone(),
        };
        match &self.splits {
            Splits::Proportions(p) => {
                for split in p.iter() {
                    let split = SplitEnum::Proportion((*split).clone());
                    let (split_sender, mut split_chunk_writers) = SplitWriter::new(
                        &output_path,
                        &split,
                        self.chunk_size,
                        self.total_rows,
                        self.output_compression,
                    )?;
                    senders.insert(split.name().to_string(), split_sender);
                    chunk_writers.append(&mut split_chunk_writers);
                }
            }
            Splits::Rows(r) => {
                for split in r.iter() {
                    let split = SplitEnum::Rows((*split).clone());
                    let (split_sender, mut split_chunk_writers) = SplitWriter::new(
                        &output_path,
                        &split,
                        self.chunk_size,
                        self.total_rows,
                        self.output_compression,
                    )?;
                    senders.insert(split.name().to_string(), split_sender);
                    chunk_writers.append(&mut split_chunk_writers);
                }
            }
        };

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(chunk_writers.len() + 2)
            .thread_name(|num| format!("thread-{num}"))
            .start_handler(|num| debug!("thread {} starting", num))
            .exit_handler(|num| debug!("thread {} finishing", num))
            .build()
            .unwrap();

        pool.scope(move |scope| {
            info!("Reading data from {}", self.input.to_str().unwrap());
            let reader_builder = if self.csv {
                let mut reader_builder = csv::ReaderBuilder::new();
                reader_builder.has_headers(false);
                Some(reader_builder)
            } else {
                None
            };
            let mut reader = open_data(&self.input, self.input_compression, reader_builder)?;

            if self.has_header {
                info!("Writing header to files");
                let header = match reader.read_line() {
                    Some(h) => h?,
                    None => return Err(Error::EmptyFile),
                };
                for sender in senders.values_mut() {
                    sender.send_all(&header)?;
                }
            }

            let has_header = self.has_header;
            {
                for writer in chunk_writers {
                    scope.spawn(move |_| {
                        // In most cases each writer will only deal with
                        // one chunk. But if we're only told a proportion and
                        // a chunk size (and no total rows), we'll be writing
                        // to two files at once, and we'll need to switch to a
                        // new file if we go over the chunk size.
                        let mut chunk_id = writer.chunk_id;
                        let mut rows_sent_to_chunk = 0;
                        let mut file = writer.output(chunk_id).expect("Could not open file");
                        let mut header: Header<String> = if has_header {
                            Header::None
                        } else {
                            Header::Disabled
                        };
                        for row in writer.receiver.iter() {
                            if header == Header::None {
                                header = Header::Some(row.clone());
                            }
                            if let Some(chunk_size) = writer.chunk_size {
                                if rows_sent_to_chunk > (chunk_size) {
                                    // add one for header
                                    // This should only ever happen if we weren't
                                    // able to pre-calculate how many chunks were
                                    // needed
                                    chunk_id = chunk_id.map(|c| c + 2);
                                    file = writer.output(chunk_id).expect("Could not open file");
                                    if let Header::Some(h) = header.as_ref() {
                                        writer
                                            .handle_row(&mut file, h)
                                            .expect("Could not write row to file");
                                    }
                                    rows_sent_to_chunk = 1
                                }
                            }
                            writer
                                .handle_row(&mut file, &row)
                                .expect("Could not write row to file");
                            rows_sent_to_chunk += 1;
                        }
                    })
                }
            }

            info!("Reading lines");
            while let Some(record) = reader.read_line() {
                let split = self.splits.get_split(&mut self.rng);
                match split {
                    SplitSelection::Some(split) => {
                        match senders.get_mut(split).unwrap().send(record.unwrap()) {
                            Ok(_) => progress[split].inc(1),
                            Err(e) => return Err(e),
                        }
                    }
                    SplitSelection::None => continue,
                    SplitSelection::Done => break,
                }
            }
            progress.values().for_each(|f| f.finish());
            info!("Finished writing to files");

            for (_, sender) in senders {
                sender.finish();
            }
            Ok(())
        })?;
        Ok(())
    }
}

#[derive(Debug, PartialEq)]
enum Header<T> {
    None,
    Some(T),
    Disabled,
}

impl Header<String> {
    fn as_ref(&self) -> Header<&str> {
        match self {
            Header::None => Header::None,
            Header::Disabled => Header::Disabled,
            Header::Some(s) => Header::Some(s.as_str()),
        }
    }
}
