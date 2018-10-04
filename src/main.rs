use env_logger;
use structopt::StructOpt;

use ttv::{cli, SplitterBuilder, Result};

fn main() -> Result<()> {
    env_logger::init();
    let opt = cli::Opt::from_args();
    match opt.cmd {
        cli::Command::Split(x) => {
            let mut splitter = SplitterBuilder::new(&x.input, x.row_splits, x.prop_splits);
            if let Some(seed) = x.seed {
                splitter = splitter.seed(seed);
            }
            splitter.build()?.run()?;
        },
    };
    Ok(())
}
