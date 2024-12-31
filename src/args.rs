use std::path::PathBuf;

use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// SQL command to execute.
    #[arg(short, long)]
    pub command: Option<Vec<String>>,

    /// Use first row as column name (if false, will use excel like column names)
    #[arg(short, long, default_value_t = false)]
    pub first_line_as_name: bool,

    /// Home directory
    #[arg(short = 'm', long)]
    pub home: Option<PathBuf>,
}
