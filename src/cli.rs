use std::path::PathBuf;

use structopt::StructOpt;

use crate::split::{RowSplit, ProportionSplit};


#[derive(Debug, StructOpt)]
#[structopt(name = "ttv", about = "Flexibly create test, train and validation sets")]
pub struct Opt {

    #[structopt(parse(from_occurrences), short = "v", help = "Set the level of verbosity")]
    pub verbose: u64,

    #[structopt(subcommand)]
    pub cmd: Command,
}

#[derive(Debug, StructOpt)]
pub enum Command {

    #[structopt(name = "split", about = "Split dataset into two or more files for test/train/validation sets")]
    Split(Split),
}

#[derive(Debug, StructOpt)]
pub struct Split {
    #[structopt(short = "r", long = "rows", required_unless = "prop_splits", conflicts_with = "prop_splits", help = "Specify splits by number of rows", use_delimiter = true)]
    pub row_splits: Vec<RowSplit>,

    #[structopt(short = "p", long = "prop", required_unless = "row_splits", help = "Specify splits by proportion of rows", use_delimiter = true)]
    pub prop_splits: Vec<ProportionSplit>,

    #[structopt(short = "c", long = "chunk-size")]
    pub chunk_size: Option<u64>,

    #[structopt(short = "t", long = "total-rows")]
    pub total_rows: Option<u64>,

    #[structopt(short = "s", long = "seed")]
    pub seed: Option<u64>,

    #[structopt(parse(from_os_str))]
    pub input: PathBuf,
}
