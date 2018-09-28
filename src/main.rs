use clap::{load_yaml, App};
use env_logger;

use ttv::{Result, SplitterBuilder};

fn main() -> Result<()> {
    env_logger::init();
    let yaml = load_yaml!("cli.yaml");
    let mut app = App::from_yaml(yaml);
    match app.clone().get_matches().subcommand() {
        ("split", Some(sub_m)) => {
            let name: &str = sub_m.value_of("INPUT").unwrap();
            let splits: Vec<&str> = sub_m.values_of("split_spec").unwrap().collect();
            let mut splitter = SplitterBuilder::new(&name, &splits)?;
            if let Some(seed) = sub_m.value_of("seed") {
                splitter = splitter.seed(seed.parse::<u64>()?);
            }
            splitter.build()?.run()?;
        },
        _ => {
            app.print_help().expect("Could not print help");
        }
    };
    Ok(())
}
