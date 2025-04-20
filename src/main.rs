use clap::Parser;
use csvsql::{
    args::Args,
    console::work_on_console,
    engine::Engine,
    error::CvsSqlError,
    writer::{Writer, new_csv_writer},
};
use std::io::{self};

fn main() -> Result<(), CvsSqlError> {
    let args = Args::parse();
    let engine = Engine::try_from(&args)?;

    if let Some(commands) = args.command {
        for command in commands {
            for results in engine.execute_commands(&command)? {
                let stdout = io::stdout().lock();
                let mut writer = new_csv_writer(stdout);
                writer.write(&results)?;
            }
        }
    } else {
        work_on_console(&engine)?;
    };

    Ok(())
}
