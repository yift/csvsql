use clap::Parser;
use csvsql::{
    args::Args,
    engine::Engine,
    error::CdvSqlError,
    writer::{new_csv_writer, Writer},
};
use std::io::{self};
use std::io::{BufRead, Write};

fn main() -> Result<(), CdvSqlError> {
    let args = Args::parse();
    let engine = Engine::try_from(&args)?;

    if let Some(commands) = args.command {
        for command in commands {
            for mut results in engine.execute_commands(&command)? {
                let stdout = io::stdout().lock();
                let mut writer = new_csv_writer(stdout);
                writer.write(&mut *results)?;
            }
        }
    } else {
        let stdin = io::stdin();
        loop {
            let mut stdout = io::stdout().lock();
            print!("{}", engine.prompt());
            stdout.flush()?;

            if let Some(line) = stdin.lock().lines().next() {
                let command = line?;

                for mut results in engine.execute_commands(&command)? {
                    let stdout = io::stdout().lock();
                    let mut writer = new_csv_writer(stdout);
                    writer.write(&mut *results)?;
                }
            } else {
                return Ok(());
            }
        }
    };

    Ok(())
}
