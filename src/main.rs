use clap::StructOpt;
use jemallocator::Jemalloc;

use ttv::{cli, Compression, Result, SplitterBuilder};

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() -> Result<()> {
    env_logger::init();
    let opt = cli::Opt::parse();
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
            if x.no_header {
                splitter = splitter.has_header(false);
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
