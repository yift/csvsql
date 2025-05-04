#![deny(warnings)]

use clap::Parser;
use csvsql::{
    args::Args, console::work_on_console, engine::Engine, error::CvsSqlError,
    outputer::create_outputer,
};

fn main() -> Result<(), CvsSqlError> {
    let args = Args::parse();
    let mut outputer = create_outputer(&args)?;
    let engine = Engine::try_from(&args)?;

    if let Some(commands) = args.command {
        for command in commands {
            for results in engine.execute_commands(&command)? {
                if let Some(out) = outputer.write(&results)? {
                    println!("{}", out);
                }
            }
        }
    } else {
        work_on_console(&engine, args.no_console, outputer)?;
    };

    Ok(())
}
