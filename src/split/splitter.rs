use std::collections::HashMap;
use std::convert::TryInto;
use std::io::BufRead;
use std::path::{Path, PathBuf};

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use log::{debug, info};
use rand::{prelude::*, prng::ChaChaRng};

use crate::error::{Error, Result};
use crate::io::open_data;
use crate::split::{
    single::{ProportionSplit, RowSplit, Split, SplitEnum},
    splits::{Splits},
    writer::SplitWriter,
};


pub struct SplitterBuilder {
    /// The path to the input file
    input: PathBuf,
    /// The desired splits
    splits: Splits,
    /// The seed used for randomisation
    seed: Option<[u8; 32]>,
    /// The prefix for the output file(s)
    output_prefix: Option<PathBuf>,
    /// The maximum size of each chunk
    chunk_size: Option<u64>,
    /// The total number of rows
    total_rows: Option<u64>,
    /// Whether to compress output files
    compressed: bool,
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
            compressed: false,
        })
    }

    pub fn seed(mut self, seed: u64) -> Self {
        let mut array: [u8; 32] = [0; 32];
        let user_seed = seed.to_le_bytes();
        array[0] = user_seed[0];
        array[1] = user_seed[1];
        array[2] = user_seed[2];
        array[3] = user_seed[3];
        array[4] = user_seed[4];
        array[5] = user_seed[5];
        array[6] = user_seed[6];
        array[7] = user_seed[7];
        self.seed = Some(array);
        self
    }

    pub fn output_prefix(mut self, output_prefix: PathBuf) -> Self {
        self.output_prefix = Some(output_prefix);
        self
    }

    pub fn chunk_size(mut self, chunk_size: u64) -> Self {
        self.chunk_size = Some(chunk_size);
        self
    }

    pub fn total_rows(mut self, total_rows: u64) -> Self {
        self.total_rows = Some(total_rows);
        self
    }

    pub fn compressed(mut self, compressed: bool) -> Self {
        self.compressed = compressed;
        self
    }

    pub fn build(self) -> Result<Splitter> {
        let rng = match self.seed {
            Some(s) => ChaChaRng::from_seed(s),
            None => ChaChaRng::from_entropy(),
        };
        Ok(Splitter {
            input: self.input,
            rng,
            splits: self.splits,
            output_prefix: self.output_prefix,
            chunk_size: self.chunk_size,
            total_rows: self.total_rows,
            compressed: self.compressed,
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
    /// Whether to compress output files
    compressed: bool,
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
                    let style = ProgressStyle::default_bar()
                        .template("{msg:<10}: [{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/~{len:7} (ETA: {eta_precise})")
                        .progress_chars("█▉▊▋▌▍▎▏  ");
                    let split_total = p.proportion * t as f64;
                    let pb = multi.add(ProgressBar::new(split_total as u64));
                    pb.set_draw_delta(10);  // uncomment when indicatif 0.9.1 is released
                    pb.set_message(&p.name());
                    pb.set_style(style);
                    (p.name().to_string(), pb)
                })
                .collect(),
            (Splits::Proportions(p), None) => p
                .splits
                .iter()
                .map(|p| {
                    let style = ProgressStyle::default_bar()
                        .template("{msg:<10}: [{elapsed_precise}] {spinner:.green} {pos:>7}");
                    let pb = multi.add(ProgressBar::new_spinner());
                    pb.set_draw_delta(10);  // uncomment when indicatif 0.9.1 is released
                    pb.set_style(style);
                    pb.set_message(&p.name());
                    (p.name().to_string(), pb)
                })
                .collect(),
            (Splits::Rows(r), _) => r
                .splits
                .iter()
                .map(|r| {
                    let style = ProgressStyle::default_bar()
                        .template("{msg:<10}: [{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} (ETA: {eta_precise})")
                        .progress_chars("█▉▊▋▌▍▎▏  ");
                    let pb = multi.add(ProgressBar::new(r.total as u64));
                    pb.set_draw_delta(10);  // uncomment when indicatif 0.9.1 is released
                    pb.set_message(&r.name());
                    pb.set_style(style);
                    (r.name().to_string().clone(), pb)
                })
                .collect()
        };

        // Change this to create senders instead
        let mut senders = HashMap::new();
        let mut chunk_writers = Vec::new();
        let output_path = match self.output_prefix {
            Some(ref f) => f.clone(),
            None => self.input.clone(),
        };
        match &self.splits {
            Splits::Proportions(p) => for split in p.iter() {
                let split = SplitEnum::Proportion((*split).clone());
                let (split_sender, mut split_chunk_writers) =
                    SplitWriter::new(&output_path, &split, self.chunk_size, self.total_rows, self.compressed)?;
                senders.insert(split.name().to_string(), split_sender);
                chunk_writers.append(&mut split_chunk_writers);
            },
            Splits::Rows(r) => for split in r.iter() {
                let split = SplitEnum::Rows((*split).clone());
                let (split_sender, mut split_chunk_writers) =
                    SplitWriter::new(&output_path, &split, self.chunk_size, self.total_rows, self.compressed)?;
                senders.insert(split.name().to_string(), split_sender);
                chunk_writers.append(&mut split_chunk_writers);
            }
        };

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(chunk_writers.len() + 2)
            .thread_name(|num| format!("thread-{}", num))
            .start_handler(|num| debug!("thread {} starting", num))
            .exit_handler(|num| debug!("thread {} finishing", num))
            .build()
            .unwrap();

        pool.scope(move |scope| {

            info!("Reading data from {}", self.input.to_str().unwrap());
            let reader = open_data(&self.input)?;

            info!("Writing header to files");
            let mut lines = reader.lines();
            let header = match lines.next() {
                None => return Err(Error::EmptyFile),
                Some(res) => res?,
            };
            for sender in senders.values_mut() {
                sender.send_all(header.clone())?;
            }

            scope.spawn(move |_| multi.join().unwrap());
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
                        for row in writer.receiver.iter() {
                            if let Some(chunk_size) = writer.chunk_size {
                                if rows_sent_to_chunk >= chunk_size {
                                    // This should only ever happen if we weren't
                                    // able to pre-calculate how many chunks were
                                    // needed
                                    chunk_id = chunk_id.map(|c| c+2);
                                    file = writer.output(chunk_id).expect("Could not open file");
                                    rows_sent_to_chunk = 0;
                                }
                            }
                            writer.handle_row(&mut file, row).expect("Could not write row to file");
                            rows_sent_to_chunk += 1;
                        }
                    })
                }
            }

            info!("Reading lines");
            for record in lines {
                let split = self.splits.get_split(&mut self.rng);
                if split.is_none() {
                    break;
                }
                let split = split.unwrap();
                match senders.get_mut(split).unwrap().send(record.unwrap()) {
                    Ok(_) => progress[split].inc(1),
                    Err(e) => return Err(e)
                }
            }
            progress.values().for_each(|f| f.finish());
            info!("Finished writing to files");

            for sender in senders.values() {
                sender.finish();
            }
            Ok(())
        })?;
        Ok(())
    }
}
