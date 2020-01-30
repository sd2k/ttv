use env_logger;
use structopt::StructOpt;

use ttv::{cli, Compression, Result, SplitterBuilder};

fn main() -> Result<()> {
    env_logger::init();
    let opt = cli::Opt::from_args();
    match opt.cmd {
        cli::Command::Split(x) => {
            let mut splitter = SplitterBuilder::new(&x.input, x.rows, x.prop)?;
            if x.decompress_input {
                splitter = splitter.input_compression(Compression::GzipCompression);
            }
            if x.compress_output {
                splitter = splitter.output_compression(Compression::GzipCompression);
            }
            if x.csv {
                splitter = splitter.csv(true);
            }
            if let Some(seed) = x.seed {
                splitter = splitter.seed(seed);
            }
            if let Some(output_prefix) = x.output_prefix {
                splitter = splitter.output_prefix(output_prefix);
            }
            if let Some(chunk_size) = x.chunk_size {
                splitter = splitter.chunk_size(chunk_size);
            }
            if let Some(total_rows) = x.total_rows {
                splitter = splitter.total_rows(total_rows);
            }
            splitter.build()?.run()?;
        }
    };
    Ok(())
}
