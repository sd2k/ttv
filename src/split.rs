use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use indicatif::ProgressBar;
use log::info;
use rand::{prelude::*, prng::ChaChaRng};

use crate::error::{Error, Result};
use crate::io::{open_data, open_output, GzWriter};

/// Represents a single 'split' of data
trait Split {
    /// Get the name of the split.
    fn name(&self) -> &str;

    /// Get the writer for this split.
    fn writer(&self, path: &PathBuf) -> Result<GzWriter> {
        let mut filename = path.clone();
        filename.pop();
        filename.push(self.name());
        filename.set_extension("csv.gz");
        open_output(filename)
    }
}

/// A split based on a proportion.
#[derive(Debug)]
pub struct ProportionSplit {
    /// The split name. Will be used as the filename for the split.
    name: String,
    /// The proportion of data that should be directed to this split.
    proportion: f64,
}

impl Split for ProportionSplit {
    fn name(&self) -> &str {
        return &self.name;
    }
}

impl FromStr for ProportionSplit {
    type Err = Error;

    /// Create a ProportionSplit from a string specification, such as
    /// "train=0.8".
    fn from_str(spec: &str) -> Result<Self> {
        let split: Vec<&str> = spec.split('=').collect();
        if split.len() != 2 {
            return Err(Error::InvalidSplitSpecification(spec.to_string()));
        }
        let proportion = split[1]
            .parse::<f64>()
            .map_err(|_| Error::InvalidSplitSpecification(spec.to_string()))?;
        Ok(ProportionSplit {
            name: split[0].to_string(),
            proportion,
        })
    }
}

/// A split based on a number of rows
#[derive(Debug)]
pub struct RowSplit {
    /// The split name. Will be used as the filename for the split.
    name: String,
    /// The total number of rows to send to this split.
    /// Stored as an f64 for optimization reasons.
    total: f64,
    /// The number of rows sent to this split so far.
    done: f64,
}

impl Split for RowSplit {
    fn name(&self) -> &str {
        return &self.name;
    }
}

impl FromStr for RowSplit {
    type Err = Error;

    /// Create a ProportionSplit from a string specification, such as
    /// "train=0.8".
    fn from_str(spec: &str) -> Result<Self> {
        let split: Vec<&str> = spec.split('=').collect();
        if split.len() != 2 {
            return Err(Error::InvalidSplitSpecification(spec.to_string()));
        }
        let total = split[1]
            .parse::<u64>()
            .map(|total| total as f64)
            .map_err(|_| Error::InvalidSplitSpecification(spec.to_string()))?;
        Ok(RowSplit {
            name: split[0].to_string(),
            total,
            done: 0.0,
        })
    }
}

trait SplitSelector {
    fn get_split(&mut self, rng: &mut ChaChaRng) -> Option<&str>;
}

/// Splits defined using proportions.
#[derive(Debug, Default)]
pub struct ProportionSplits {
    splits: Vec<ProportionSplit>,
}

impl SplitSelector for ProportionSplits {
    fn get_split(&mut self, rng: &mut ChaChaRng) -> Option<&str> {
        let random: f64 = rng.gen();
        let mut total = 0.0;
        for split in &self.splits {
            total += split.proportion;
            if random < total {
                return Some(&split.name);
            }
        }
        unreachable!()
    }
}

impl From<Vec<ProportionSplit>> for ProportionSplits {
    fn from(splits: Vec<ProportionSplit>) -> Self {
        ProportionSplits { splits }
    }
}

/// Splits defined using rows.
#[derive(Debug, Default)]
pub struct RowSplits {
    splits: Vec<RowSplit>,
    /// The total number of rows in all splits combined
    total: f64,
}

impl SplitSelector for RowSplits {
    fn get_split(&mut self, rng: &mut ChaChaRng) -> Option<&str> {
        let random: f64 = rng.gen();
        let random = random * self.total;

        let mut total = 0.0;
        let unfinished_splits: Vec<&mut RowSplit> = self
            .splits
            .iter_mut()
            .filter(|s| s.done < s.total)
            .collect();

        for split in unfinished_splits.into_iter() {
            total += split.total;
            if random < total {
                split.done += 1.0;
                if split.done >= split.total {
                    self.total -= split.total;
                }
                return Some(split.name.as_ref());
            }
        }
        return None;
    }
}

impl From<Vec<RowSplit>> for RowSplits {
    fn from(splits: Vec<RowSplit>) -> Self {
        let total = splits.iter().fold(0.0, |x, y| x + y.total);
        RowSplits { splits, total }
    }
}

/// Either RowSplits or ProportionSplits, determined at runtime depending
/// on the user's input.
pub enum Splits {
    Rows(RowSplits),
    Proportions(ProportionSplits),
}

impl Splits {
    /// Get a mapping from a split's name to its file.
    pub fn outputs(&self, data: &Path) -> Result<HashMap<String, GzWriter>> {
        match self {
            Splits::Rows(rows) => rows
                .splits
                .iter()
                .map(|s| Ok((s.name.clone(), s.writer(&data.to_path_buf())?)))
                .collect(),
            Splits::Proportions(props) => props
                .splits
                .iter()
                .map(|s| Ok((s.name.clone(), s.writer(&data.to_path_buf())?)))
                .collect(),
        }
    }

    /// Get a random split.
    pub fn get_split(&mut self, rng: &mut ChaChaRng) -> Option<&str> {
        match self {
            Splits::Rows(rows) => rows.get_split(rng),
            Splits::Proportions(rows) => rows.get_split(rng),
        }
    }
}

pub struct SplitterBuilder {
    /// The path to the data file
    data: PathBuf,
    /// The desired splits
    splits: Splits,
    /// The seed used for randomisation
    seed: Option<[u8; 32]>,
}

impl SplitterBuilder {
    pub fn new<P: AsRef<Path>>(
        data: &P,
        row_splits: Vec<RowSplit>,
        prop_splits: Vec<ProportionSplit>,
    ) -> Self {
        let splits = if row_splits.is_empty() {
            Splits::Proportions(prop_splits.into())
        } else {
            Splits::Rows(row_splits.into())
        };
        SplitterBuilder {
            data: data.as_ref().to_path_buf(),
            splits,
            seed: None,
        }
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

    pub fn build(self) -> Result<Splitter> {
        let rng = match self.seed {
            Some(s) => ChaChaRng::from_seed(s),
            None => ChaChaRng::from_entropy(),
        };
        Ok(Splitter {
            data: self.data,
            rng,
            splits: self.splits,
        })
    }
}

pub struct Splitter {
    /// The path to the data file
    data: PathBuf,
    /// The desired splits
    splits: Splits,
    /// The stateful random number generator.
    rng: ChaChaRng,
}

impl Splitter {
    pub fn run(mut self) -> Result<()> {

        let mut outputs: HashMap<String, GzWriter> = self
            .splits
            .outputs(&self.data)?;

        let pb = match &self.splits {
            Splits::Proportions(_) => ProgressBar::new_spinner(),
            Splits::Rows(r) => ProgressBar::new(r.total as u64)
        };

        info!("Reading data from {}", self.data.to_str().unwrap());
        let reader = open_data(&self.data)?;

        info!("Writing header to files");
        let mut lines = reader.lines();
        let header = match lines.next() {
            None => return Err(Error::EmptyFile),
            Some(res) => res?,
        };
        for output in outputs.values_mut() {
            output.write_all(&header.clone().into_bytes())?;
            output.write_all("\n".as_bytes())?;
        }

        // let outputs: Arc<Mutex<_>> = Arc::new(Mutex::new(self.outputs));

        let (tx, rx) = std::sync::mpsc::channel::<(String, String)>();

        let outputs = outputs;

        let mut handles = Vec::new();
        handles.push(std::thread::spawn(move || {
            let mut outputs = outputs;
            for (split_name, record) in rx.iter() {
                let output = outputs.get_mut(&split_name).expect("Could not find output");
                output.write_all(&record.into_bytes()).expect("Could not write to file");
                output.write_all("\n".as_bytes()).expect("Could not write to file");
            }
        }));

        info!("Writing to files");
        for result in lines {
            pb.inc(1);
            let record = result?;
            let split = self.splits.get_split(&mut self.rng);
            if split.is_none() {
                break;
            }
            let split = split.unwrap();

            tx.send((split.to_string(), record)).expect("Could not send message");
        }
        info!("Finished writing to files");

        drop(tx);
        for handle in handles {
            handle.join().unwrap();
        }
        Ok(())
    }
}
