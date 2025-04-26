use std::path::PathBuf;

use clap::Parser;

#[derive(Parser, Debug, Default)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// SQL command to execute.
    #[arg(short, long)]
    pub command: Option<Vec<String>>,

    /// Use excel like column name (if not set, the first line of the file will be the column name)
    #[arg(short, long, default_value_t = false)]
    pub first_line_as_data: bool,

    /// Home directory
    #[arg(short = 'm', long)]
    pub home: Option<PathBuf>,

    /// Run with simple stdio
    #[arg(short, long, default_value_t = false)]
    pub no_console: bool,

    /// Display output as CSV in console (valid only in console mode)
    #[arg(short, long, default_value_t = false)]
    pub display_as_csv: bool,
}
