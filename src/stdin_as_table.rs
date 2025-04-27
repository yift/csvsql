use std::io::Read;
use std::{io, path::PathBuf};

use tempfile::NamedTempFile;

use crate::error::CvsSqlError;

pub(crate) trait StdinReader {
    fn path(&mut self) -> Result<PathBuf, CvsSqlError>;
}

pub(crate) fn create_stdin_reader(supported: bool) -> Box<dyn StdinReader> {
    if supported {
        let reader = io::stdin().lock();
        Box::new(StdinAsTableState::Unread(reader))
    } else {
        Box::new(UnsupportedStdinReader {})
    }
}
enum StdinAsTableState<R: Read> {
    Unread(R),
    Read(NamedTempFile),
}

impl<R: Read> StdinReader for StdinAsTableState<R> {
    fn path<'a>(&mut self) -> Result<PathBuf, CvsSqlError> {
        match self {
            Self::Read(r) => Ok(r.path().to_path_buf()),
            Self::Unread(r) => {
                let mut temporary_file = NamedTempFile::with_suffix(".csv")?;
                io::copy(r, &mut temporary_file)?;
                *self = Self::Read(temporary_file);
                self.path()
            }
        }
    }
}

struct UnsupportedStdinReader {}
impl StdinReader for UnsupportedStdinReader {
    fn path(&mut self) -> Result<PathBuf, CvsSqlError> {
        Err(CvsSqlError::StdinUnusable)
    }
}
