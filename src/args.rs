use std::path::PathBuf;

use clap::Parser;
use clap::ValueEnum;

#[derive(Debug, Clone, ValueEnum, Default)]
#[clap(rename_all = "kebab_case")]
pub enum OutputFormat {
    /// CSV file format
    #[default]
    Csv,
    /// Tab-separated text
    Txt,
    /// HTML files
    Html,
    /// JSON files
    Json,
    /// Excel sheet
    Xls,
}

#[derive(Parser, Debug, Default)]
#[command(
    version,
    about = "CSV SQL-like engine for command line CSV manipulation",
    long_about = "A SQL-like engine for reading and manipulating CSV files from the command line.\n\
                  A fast alternative to Excel/LibreOffice for CSV operations."
)]
pub struct Args {
    /// SQL command to execute. If omitted, the engine will read commands from standard input.
    /// If set, standard input can be used as a table named '$'.
    #[arg(short, long)]
    pub command: Option<Vec<String>>,

    /// Use Excel-like column names (if not set, the first line of each file will be used as column names)
    #[arg(short, long, default_value_t = false)]
    pub first_line_as_data: bool,

    /// Home directory (base path for CSV files and databases). Defaults to current directory.
    #[arg(short = 'm', long)]
    #[arg(value_hint = clap::ValueHint::DirPath)]
    pub home: Option<PathBuf>,

    /// Disable interactive terminal mode and use simple stdio (for pipes and scripts)
    #[arg(short, long, default_value_t = false)]
    pub no_console: bool,

    /// Output directory for saving results
    #[arg(short, long)]
    #[arg(value_hint = clap::ValueHint::DirPath)]
    pub output: Option<PathBuf>,

    /// Output format when saving to files
    #[arg(short='p', long, value_enum, default_value_t=OutputFormat::Csv)]
    pub output_format: OutputFormat,

    /// Display output as CSV in console instead of as a table (valid only in console mode)
    #[arg(short, long, default_value_t = false)]
    pub display_as_csv: bool,

    /// Enable write mode to allow modifying files
    #[arg(short, long, default_value_t = false)]
    pub write_mode: bool,
}
