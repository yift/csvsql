use std::{fs, rc::Rc};

use sqlparser::ast::{ObjectName, ObjectType};

use crate::{
    engine::Engine,
    error::CvsSqlError,
    result_set_metadata::SimpleResultSetMetadata,
    results::ResultSet,
    results_data::{DataRow, ResultsData},
    value::Value,
};

#[allow(clippy::too_many_arguments)]
pub(crate) fn drop_table(
    engine: &Engine,
    object_type: &ObjectType,
    if_exists: &bool,
    names: &[ObjectName],
    cascade: &bool,
    restrict: &bool,
    purge: &bool,
    temporary: &bool,
) -> Result<ResultSet, CvsSqlError> {
    if object_type != &ObjectType::Table {
        return Err(CvsSqlError::Unsupported(format!("DROP {}", object_type)));
    }
    if names.is_empty() {
        return Err(CvsSqlError::Unsupported("DROP without tables".to_string()));
    }
    if *cascade {
        return Err(CvsSqlError::Unsupported("DROP CASCADE".to_string()));
    }
    if *restrict {
        return Err(CvsSqlError::Unsupported("DROP RESTRICT".to_string()));
    }
    if *purge {
        return Err(CvsSqlError::Unsupported("DROP PURGE".to_string()));
    }

    let mut files = vec![];
    for name in names {
        let file = engine.file_name(name)?;
        if file.read_only {
            return Err(CvsSqlError::ReadOnlyMode);
        }
        if file.exists {
            files.push(file);
        } else if !if_exists {
            return Err(CvsSqlError::TableNotExists(file.result_name.full_name()));
        }
    }
    let mut metadata = SimpleResultSetMetadata::new(None);
    metadata.add_column("action");
    metadata.add_column("table");
    metadata.add_column("file");
    let metadata = metadata.build();

    let mut data = vec![];
    for file in files {
        if file.is_temp {
            engine.drop_temporary_table(&file)?;
        } else {
            if *temporary {
                return Err(CvsSqlError::TableNotTemporary(file.result_name.full_name()));
            }
            fs::remove_file(&file.path)?;
        }
        let file_name = engine.get_file_name(&file);
        let row = vec![
            Value::Str("DROPPED".to_string()),
            Value::Str(file.result_name.full_name()),
            Value::Str(file_name.to_string()),
        ];
        let row = DataRow::new(row);
        data.push(row);
    }

    let data = ResultsData::new(data);
    let metadata = Rc::new(metadata);
    let results = ResultSet { metadata, data };

    Ok(results)
}
#[cfg(test)]
mod tests {

    use sqlparser::ast::Ident;

    use crate::args::Args;

    use super::*;

    #[test]
    fn drop_empty_list() -> Result<(), CvsSqlError> {
        let args = Args {
            writer_mode: true,
            ..Args::default()
        };

        let engine = Engine::try_from(&args)?;
        let object_type = ObjectType::Table;
        let if_exists = true;
        let names = vec![];
        let cascade = false;
        let restrict = false;
        let purge = false;
        let temporary = true;

        let Err(err) = drop_table(
            &engine,
            &object_type,
            &if_exists,
            &names,
            &cascade,
            &restrict,
            &purge,
            &temporary,
        ) else {
            panic!("Expecting an error");
        };

        assert!(matches!(err, CvsSqlError::Unsupported(_)));

        Ok(())
    }

    #[test]
    fn drop_empty_temp_not_a_temp() -> Result<(), CvsSqlError> {
        let args = Args {
            writer_mode: true,
            ..Args::default()
        };

        let engine = Engine::try_from(&args)?;
        let object_type = ObjectType::Table;
        let if_exists = true;
        let ident = vec![Ident::new("tests"), Ident::new("data"), Ident::new("sales")];

        let names = vec![ident.into()];
        let cascade = false;
        let restrict = false;
        let purge = false;
        let temporary = true;

        let Err(err) = drop_table(
            &engine,
            &object_type,
            &if_exists,
            &names,
            &cascade,
            &restrict,
            &purge,
            &temporary,
        ) else {
            panic!("Expecting an error");
        };

        assert!(matches!(err, CvsSqlError::TableNotTemporary(_)));

        Ok(())
    }
}
