use std::fs::{self, File};
use std::rc::Rc;

use sqlparser::ast::CreateTable;

use crate::cast::AvailableDataTypes;
use crate::engine::Engine;
use crate::error::CvsSqlError;
use crate::extractor::Extractor;
use crate::file_results::read_file;
use crate::result_set_metadata::{Metadata, SimpleResultSetMetadata};
use crate::results::ResultSet;
use crate::results_data::{DataRow, ResultsData};
use crate::value::Value;
use crate::writer::{Writer, new_csv_writer};

impl Extractor for CreateTable {
    fn extract(&self, engine: &Engine) -> Result<ResultSet, CvsSqlError> {
        if self.external {
            return Err(CvsSqlError::Unsupported("CREATE EXTERNAL TABLE".into()));
        }
        if self.global == Some(true) {
            return Err(CvsSqlError::Unsupported("CREATE GLOBAL TABLE".into()));
        }
        if self.or_replace {
            return Err(CvsSqlError::Unsupported("CREATE OR REPLACE".into()));
        }
        if self.transient {
            return Err(CvsSqlError::Unsupported("CREATE TRANSIENT TABLE".into()));
        }
        if self.volatile {
            return Err(CvsSqlError::Unsupported("CREATE VOLOTILE TABLE".into()));
        }
        if self.iceberg {
            return Err(CvsSqlError::Unsupported("CREATE ICEBERG table".into()));
        }
        if !self.constraints.is_empty() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with constraints".into(),
            ));
        }
        if !self.table_properties.is_empty() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with properties".into(),
            ));
        }
        if !self.with_options.is_empty() {
            return Err(CvsSqlError::Unsupported("CREATE TABLE with options".into()));
        }
        if self.file_format.is_some() {
            return Err(CvsSqlError::Unsupported("CREATE TABLE with format".into()));
        }
        if self.location.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with location".into(),
            ));
        }
        if self.engine.is_some() {
            return Err(CvsSqlError::Unsupported("CREATE TABLE with engine".into()));
        }
        if self.comment.is_some() {
            return Err(CvsSqlError::Unsupported("CREATE TABLE with comment".into()));
        }
        if self.auto_increment_offset.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with auto increment offset".into(),
            ));
        }
        if self.default_charset.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with default charset".into(),
            ));
        }
        if self.collation.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with collation".into(),
            ));
        }
        if self.on_commit.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with on commit".into(),
            ));
        }
        if self.on_cluster.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with on cluster".into(),
            ));
        }
        if self.primary_key.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with PRIMARY KEY".into(),
            ));
        }
        if self.order_by.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with order by".into(),
            ));
        }
        if self.partition_by.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with partition by".into(),
            ));
        }
        if self.cluster_by.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with cluster by".into(),
            ));
        }
        if self.clustered_by.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with clustered by".into(),
            ));
        }
        if self.options.is_some() {
            return Err(CvsSqlError::Unsupported("CREATE TABLE with options".into()));
        }
        if self.strict {
            return Err(CvsSqlError::Unsupported("CREATE TABLE with strict".into()));
        }
        if self.copy_grants {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with copy grants".into(),
            ));
        }

        if self.enable_schema_evolution.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with enable_schema_evolution".into(),
            ));
        }
        if self.change_tracking.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with change_tracking".into(),
            ));
        }
        if self.data_retention_time_in_days.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with data_retention_time_in_days".into(),
            ));
        }
        if self.max_data_extension_time_in_days.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with max_data_extension_time_in_days".into(),
            ));
        }
        if self.default_ddl_collation.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with default_ddl_collation".into(),
            ));
        }
        if self.with_aggregation_policy.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with with_aggregation_policy".into(),
            ));
        }
        if self.with_row_access_policy.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with with_row_access_policy".into(),
            ));
        }
        if self.with_tags.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with with_tags".into(),
            ));
        }
        if self.external_volume.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with external_volume".into(),
            ));
        }
        if self.base_location.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with base_location".into(),
            ));
        }
        if self.catalog.is_some() {
            return Err(CvsSqlError::Unsupported("CREATE TABLE with catalog".into()));
        }
        if self.catalog_sync.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with catalog_sync".into(),
            ));
        }
        if self.storage_serialization_policy.is_some() {
            return Err(CvsSqlError::Unsupported(
                "CREATE TABLE with storage_serialization_policy".into(),
            ));
        }

        let file = if self.temporary {
            engine.create_temp_file(&self.name)?
        } else {
            engine.file_name(&self.name)?
        };
        if file.read_only {
            return Err(CvsSqlError::ReadOnlyMode);
        }
        if file.is_temp && !self.temporary {
            return Err(CvsSqlError::TemporaryTableyExists(
                file.result_name.full_name(),
            ));
        }
        let data = if !self.columns.is_empty() {
            let mut metadata = SimpleResultSetMetadata::new(None);
            for col in &self.columns {
                AvailableDataTypes::try_from(&col.data_type)?;
                metadata.add_column(&col.name.to_string());
            }
            let metadata = Rc::new(metadata.build());
            ResultSet {
                metadata,
                data: ResultsData::new(vec![]),
            }
        } else if let Some(query) = &self.query {
            query.extract(engine)?
        } else if let Some(like) = &self.like {
            let data = read_file(engine, like)?;
            ResultSet {
                metadata: data.metadata.clone(),
                data: ResultsData::new(vec![]),
            }
        } else if let Some(clone) = &self.clone {
            read_file(engine, clone)?
        } else {
            return Err(CvsSqlError::NoTableStructuye(file.result_name.full_name()));
        };

        let file_name = engine.get_file_name(&file);
        let table_name = file.result_name.full_name();
        if file.exists {
            if !self.if_not_exists {
                return Err(CvsSqlError::TableAlreadyExists(table_name));
            }
        } else {
            if let Some(parent) = file.path.parent() {
                fs::create_dir_all(parent)?;
            }
            let writer = File::create(file.path)?;
            let mut writer = new_csv_writer(writer, engine.first_line_as_name);
            writer.write(&data)?;
        }

        let mut metadata = SimpleResultSetMetadata::new(None);
        metadata.add_column("action");
        metadata.add_column("table");
        metadata.add_column("file");
        let metadata = Metadata::Simple(metadata);

        let row = vec![
            Value::Str("CREATED".to_string()),
            Value::Str(table_name),
            Value::Str(file_name),
        ];
        let row = DataRow::new(row);
        let data = vec![row];
        let data = ResultsData::new(data);
        let metadata = Rc::new(metadata);
        let results = ResultSet { metadata, data };

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::{
        ast::{
            ClusteredBy, CommentDef, Expr, Ident, OneOrManyWithParens, RowAccessPolicy, SqlOption,
            Statement, StorageSerializationPolicy, TableEngine, TableOptionsClustered, Value,
            ValueWithSpan,
        },
        parser::Parser,
        tokenizer::Span,
    };

    use crate::{args::Args, dialect::FilesDialect};

    use super::*;

    fn test_unsupported(change: fn(&mut CreateTable)) -> Result<(), CvsSqlError> {
        let args = Args {
            writer_mode: true,
            ..Args::default()
        };

        let engine = Engine::try_from(&args)?;

        let sql = "CREATE TABLE test_one(col TEXT)";
        let dialect = FilesDialect {};
        let statement = Parser::parse_sql(&dialect, sql)?;
        let Some(Statement::CreateTable(mut create)) = statement.into_iter().next() else {
            panic!("Not a create statement");
        };
        change(&mut create);

        let Err(err) = create.extract(&engine) else {
            panic!("No error");
        };

        assert!(matches!(err, CvsSqlError::Unsupported(_)));

        Ok(())
    }

    #[test]
    fn external_table_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.external = true;
        })
    }

    #[test]
    fn volatile_table_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.volatile = true;
        })
    }

    #[test]
    fn iceberg_table_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.iceberg = true;
        })
    }

    #[test]
    fn table_with_properties_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.table_properties = vec![SqlOption::Clustered(
                TableOptionsClustered::ColumnstoreIndex,
            )]
        })
    }

    #[test]
    fn table_with_option_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.with_options = vec![SqlOption::Clustered(
                TableOptionsClustered::ColumnstoreIndex,
            )]
        })
    }

    #[test]
    fn table_option_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.options = Some(vec![SqlOption::Ident(Ident::new("test"))]);
        })
    }

    #[test]
    fn table_with_file_format_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| create.file_format = Some(sqlparser::ast::FileFormat::JSONFILE))
    }

    #[test]
    fn table_location_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| create.location = Some("location".to_string()))
    }

    #[test]
    fn table_engine_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.engine = Some(TableEngine {
                name: "name".to_string(),
                parameters: None,
            })
        })
    }

    #[test]
    fn table_comment_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| create.comment = Some(CommentDef::WithEq("location".to_string())))
    }

    #[test]
    fn table_auto_increment_offset_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| create.auto_increment_offset = Some(20))
    }

    #[test]
    fn table_default_charset_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| create.default_charset = Some("utf8".to_string()))
    }

    #[test]
    fn table_collation_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| create.collation = Some("collation".to_string()))
    }

    #[test]
    fn table_primary_key_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            let expr = Expr::Value(ValueWithSpan {
                value: Value::Null,
                span: Span::empty(),
            });
            let expr = Box::new(expr);

            create.primary_key = Some(expr)
        })
    }

    #[test]
    fn table_order_by_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            let expr = Expr::Value(ValueWithSpan {
                value: Value::Null,
                span: Span::empty(),
            });

            create.order_by = Some(OneOrManyWithParens::One(expr));
        })
    }

    #[test]
    fn table_partition_by_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            let expr = Expr::Value(ValueWithSpan {
                value: Value::Null,
                span: Span::empty(),
            });
            let expr = Box::new(expr);

            create.partition_by = Some(expr)
        })
    }

    #[test]
    fn table_cluster_by_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            let ident = Ident::new("value");

            create.cluster_by = Some(sqlparser::ast::WrappedCollection::NoWrapping(vec![ident]))
        })
    }

    #[test]
    fn table_clustered_by_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            let cluster_by = ClusteredBy {
                columns: vec![],
                sorted_by: None,
                num_buckets: Value::Null,
            };

            create.clustered_by = Some(cluster_by)
        })
    }

    #[test]
    fn strict_table_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.strict = true;
        })
    }

    #[test]
    fn copy_grants_table_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.copy_grants = true;
        })
    }

    #[test]
    fn enable_schema_evolution_table_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.enable_schema_evolution = Some(true);
        })
    }

    #[test]
    fn change_tracking_table_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.change_tracking = Some(true);
        })
    }

    #[test]
    fn data_retention_time_in_days_table_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.data_retention_time_in_days = Some(2);
        })
    }

    #[test]
    fn max_data_extension_time_in_days_table_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.max_data_extension_time_in_days = Some(2);
        })
    }

    #[test]
    fn default_ddl_collation_table_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.default_ddl_collation = Some("one".into());
        })
    }

    #[test]
    fn with_aggregation_policy_table_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.with_aggregation_policy = Some(vec![].into());
        })
    }

    #[test]
    fn with_row_access_policy_table_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.with_row_access_policy = Some(RowAccessPolicy::new(vec![].into(), vec![]));
        })
    }

    #[test]
    fn with_tags_table_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.with_tags = Some(vec![]);
        })
    }

    #[test]
    fn base_location_table_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.base_location = Some("location".into());
        })
    }

    #[test]
    fn catalog_table_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.catalog = Some("location".into());
        })
    }

    #[test]
    fn catalog_sync_table_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.catalog_sync = Some("location".into());
        })
    }

    #[test]
    fn storage_serialization_policy_table_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.storage_serialization_policy = Some(StorageSerializationPolicy::Compatible);
        })
    }

    #[test]
    fn external_volume_table_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported(|create| {
            create.external_volume = Some("location".into());
        })
    }
}
