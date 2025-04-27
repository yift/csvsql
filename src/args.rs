use std::path::PathBuf;

use clap::Parser;
use clap::ValueEnum;

#[derive(Debug, Clone, ValueEnum, Default)]
#[clap(rename_all = "kebab_case")]
pub enum OutputFormat {
    /// CSV file format
    #[default]
    Csv,
    /// Tab seperated text
    Txt,
    /// HTML files
    Html,
    /// JSON files
    Json,
    /// Excel sheet
    Xls,
}

#[derive(Parser, Debug, Default)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// SQL command to execute. If ommited the engine will read the command from the standart input. If set, the standard input can be used as table named '$'.
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

    /// Output directory
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// The output format when output is set
    #[arg(short='p', long, value_enum, default_value_t=OutputFormat::Csv)]
    pub output_format: OutputFormat,

    /// Display output as CSV in console (valid only in console mode)
    #[arg(short, long, default_value_t = false)]
    pub display_as_csv: bool,

    /// Allow to modify file
    #[arg(short, long, default_value_t = false)]
    pub writer_mode: bool,
}
