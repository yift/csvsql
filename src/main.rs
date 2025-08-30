#![deny(warnings)]

use std::process::exit;

use clap::Parser;
use csvsql::{
    args::Args, console::work_on_console, engine::Engine, error::CvsSqlError,
    outputer::create_outputer,
};

fn main() {
    if let Err(err) = run() {
        eprintln!("{err}");
        exit(-1);
    };
}

fn run() -> Result<(), CvsSqlError> {
    let args = Args::parse();
    let mut outputer = create_outputer(&args)?;
    let engine = Engine::try_from(&args)?;

    if let Some(commands) = args.command {
        for command in commands {
            for results in engine.execute_commands(&command)? {
                if let Some(out) = outputer.write(&results)? {
                    println!("{out}");
                }
            }
        }
    } else {
        work_on_console(&engine, args.no_console, outputer.as_mut())?;
    };

    Ok(())
}
