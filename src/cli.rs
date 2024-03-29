use std::path::PathBuf;

use clap::StructOpt;

use crate::split::{ProportionSplit, RowSplit};

#[derive(Debug, StructOpt)]
#[clap(
    name = "ttv",
    about = "Flexibly create test, train and validation sets"
)]
pub struct Opt {
    #[clap(
        parse(from_occurrences),
        short = 'v',
        help = "Set the level of verbosity"
    )]
    pub verbose: u64,

    #[clap(subcommand)]
    pub cmd: Command,
}

#[derive(Debug, StructOpt)]
pub enum Command {
    #[clap(
        name = "split",
        about = "Split dataset into two or more files for test/train/validation sets"
    )]
    Split(Split),
}

#[derive(Debug, StructOpt)]
pub struct Split {
    #[clap(
        short = 'r',
        long = "rows",
        required_unless_present = "prop",
        conflicts_with = "prop",
        help = "Specify splits by number of rows",
        use_value_delimiter = true
    )]
    pub rows: Vec<RowSplit>,

    #[clap(
        short = 'p',
        long = "prop",
        required_unless_present = "rows",
        conflicts_with = "rows",
        help = "Specify splits by proportion of rows",
        use_value_delimiter = true
    )]
    pub prop: Vec<ProportionSplit>,

    #[clap(
        short = 'n',
        long = "no-header",
        help = "Don't treat the first row as a header"
    )]
    pub no_header: bool,

    #[clap(
        short = 'c',
        long = "chunk-size",
        help = "Maximum number of rows per output chunk"
    )]
    pub chunk_size: Option<u64>,

    #[clap(
        short = 't',
        long = "total-rows",
        help = "Number of rows in input file. Used for progress when using proportion splits"
    )]
    pub total_rows: Option<u64>,

    #[clap(short = 's', long = "seed", help = "RNG seed, for reproducibility")]
    pub seed: Option<u64>,

    #[clap(
        long = "csv",
        help = "Parse input as CSV. Only needed if rows contain embedded newlines - will impact performance."
    )]
    pub csv: bool,

    #[clap(
        parse(from_os_str),
        help = "Data to split, optionally gzip compressed. If '-', read from stdin"
    )]
    pub input: PathBuf,

    #[clap(
        short = 'o',
        long = "output-prefix",
        parse(from_os_str),
        required_if_eq("input", "-"),
        help = "Output filename prefix. Only used if reading from stdin"
    )]
    pub output_prefix: Option<PathBuf>,

    #[clap(
        short = 'd',
        long = "decompress-input",
        help = "Decompress input from gzip format"
    )]
    pub decompress_input: bool,

    #[clap(
        short = 'C',
        long = "compressed-output",
        help = "Compress output files using gzip"
    )]
    pub compress_output: bool,
}
