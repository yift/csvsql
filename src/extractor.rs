use sqlparser::ast::{
    Expr, GroupByExpr, LimitClause, OrderBy, Query, Select, SetExpr, Statement, TableFactor, Use,
};

use crate::alter::alter;
use crate::drop::drop_table;
use crate::error::CvsSqlError;
use crate::file_results::read_file;
use crate::filter_results::{apply_having, make_filter};
use crate::group_by::{force_group_by, group_by};
use crate::join::create_join;
use crate::named_results::alias_results;
use crate::order_by_results::order_by;
use crate::projections::make_projection;
use crate::show::{show_databases, show_tables};
use crate::transaction::{commit_transaction, rollback_transaction, start_transaction};
use crate::trimmer::trim;
use crate::update::update_table;
use crate::{engine::Engine, results::ResultSet};
pub trait Extractor {
    fn extract(&self, engine: &Engine) -> Result<ResultSet, CvsSqlError>;
}

impl Extractor for Statement {
    fn extract(&self, engine: &Engine) -> Result<ResultSet, CvsSqlError> {
        match self {
            Statement::Query(query) => query.extract(engine),
            Statement::CreateTable(table) => table.extract(engine),
            Statement::Insert(insert) => insert.extract(engine),
            Statement::Update {
                table,
                assignments,
                from: _,
                selection,
                returning,
                or,
            } => update_table(engine, table, assignments, selection, returning, or),
            Statement::Drop {
                object_type,
                if_exists,
                names,
                cascade,
                restrict,
                purge,
                temporary,
                table,
            } => drop_table(
                engine,
                object_type,
                if_exists,
                names,
                cascade,
                restrict,
                purge,
                temporary,
                table,
            ),
            Statement::Delete(delete) => delete.extract(engine),
            Statement::AlterTable {
                name,
                if_exists,
                only: _,
                operations,
                location,
                on_cluster,
                iceberg,
            } => alter(
                engine, name, *if_exists, operations, location, on_cluster, iceberg,
            ),
            Statement::StartTransaction {
                modes,
                begin: _,
                transaction,
                modifier,
                statements,
                exception_statements,
                has_end_keyword: _,
            } => start_transaction(
                engine,
                modes,
                transaction,
                modifier,
                statements,
                exception_statements,
            ),
            Statement::Commit {
                chain: _,
                end: _,
                modifier,
            } => commit_transaction(engine, modifier),
            Statement::Rollback {
                chain: _,
                savepoint,
            } => rollback_transaction(engine, savepoint),
            Statement::Use(name) => {
                let Use::Object(name) = name else {
                    return Err(CvsSqlError::Unsupported(self.to_string()));
                };
                engine.change_home(name)
            }
            Statement::ShowTables {
                terse,
                history,
                extended,
                full,
                external,
                show_options: _,
            } => {
                if *terse {
                    return Err(CvsSqlError::Unsupported("SHOW TERSE TABLES".to_string()));
                }
                if *history {
                    return Err(CvsSqlError::Unsupported("SHOW HISTORY TABLES".to_string()));
                }
                if *extended {
                    return Err(CvsSqlError::Unsupported("SHOW EXTENDED TABLES".to_string()));
                }
                if *external {
                    return Err(CvsSqlError::Unsupported("SHOW EXTERNAL TABLES".to_string()));
                }
                show_tables(engine, full)
            }
            Statement::ShowDatabases {
                terse,
                history,
                show_options: _,
            } => {
                if *terse {
                    return Err(CvsSqlError::Unsupported("SHOW TERSE DATABASES".to_string()));
                }
                if *history {
                    return Err(CvsSqlError::Unsupported(
                        "SHOW HISTORY DATABASES".to_string(),
                    ));
                }
                show_databases(engine)
            }
            _ => Err(CvsSqlError::Unsupported(self.to_string())),
        }
    }
}

impl Extractor for Query {
    fn extract(&self, engine: &Engine) -> Result<ResultSet, CvsSqlError> {
        if self.fetch.is_some() {
            return Err(CvsSqlError::Unsupported("SELECT ... FETCH".to_string()));
        }
        if self.for_clause.is_some() {
            return Err(CvsSqlError::Unsupported("SELECT ... FOR".to_string()));
        }
        if self.with.is_some() {
            return Err(CvsSqlError::Unsupported("SELECT ... WITH".to_string()));
        }
        let (limit, offset) = match &self.limit_clause {
            None => (None, None),
            Some(LimitClause::OffsetCommaLimit { offset, limit }) => (Some(offset), Some(limit)),
            Some(LimitClause::LimitOffset {
                limit,
                offset,
                limit_by,
            }) => {
                if !limit_by.is_empty() {
                    return Err(CvsSqlError::Unsupported("SELECT ... LIMIT BY".to_string()));
                }
                let offset = offset.as_ref().map(|o| &o.value);
                let limit = limit.as_ref();
                (limit, offset)
            }
        };
        if !self.locks.is_empty() {
            return Err(CvsSqlError::Unsupported(
                "SELECT ... FOR UPDATE/SHARE".to_string(),
            ));
        }
        if self.settings.is_some() {
            return Err(CvsSqlError::Unsupported("SELECT ... SETTINGS".to_string()));
        }
        if self.format_clause.is_some() {
            return Err(CvsSqlError::Unsupported("SELECT ... FORMAT".to_string()));
        }

        match &*self.body {
            SetExpr::Select(select) => {
                extract(select, &self.order_by, limit, offset, engine, false)
            }
            SetExpr::Values(values) => values.extract(engine),
            _ => Err(CvsSqlError::Unsupported(format!("SELECT {}", self.body))),
        }
    }
}
impl Extractor for Select {
    fn extract(&self, engine: &Engine) -> Result<ResultSet, CvsSqlError> {
        extract(self, &None, None, None, engine, false)
    }
}
fn extract(
    select: &Select,
    order: &Option<OrderBy>,
    limit: Option<&Expr>,
    offset: Option<&Expr>,
    engine: &Engine,
    force_group: bool,
) -> Result<ResultSet, CvsSqlError> {
    if select.distinct.is_some() {
        return Err(CvsSqlError::Unsupported("SELECT DISTINCT".to_string()));
    }
    if select.top.is_some() {
        return Err(CvsSqlError::Unsupported("SELECT TOP".to_string()));
    }
    if select.into.is_some() {
        return Err(CvsSqlError::Unsupported("SELECT INTO".to_string()));
    }
    if !select.lateral_views.is_empty() {
        return Err(CvsSqlError::Unsupported("SELECT LATERAL VIEW".to_string()));
    }
    if select.prewhere.is_some() {
        return Err(CvsSqlError::Unsupported("SELECT ... PREWHERE".to_string()));
    }
    if !select.cluster_by.is_empty() {
        return Err(CvsSqlError::Unsupported(
            "SELECT ... CLUSTER BY".to_string(),
        ));
    }
    if !select.distribute_by.is_empty() {
        return Err(CvsSqlError::Unsupported(
            "SELECT ... DISTRIBUTE BY".to_string(),
        ));
    }
    if !select.sort_by.is_empty() {
        return Err(CvsSqlError::Unsupported("SELECT ... SORT BY".to_string()));
    }
    if !select.named_window.is_empty() || select.window_before_qualify {
        return Err(CvsSqlError::Unsupported("SELECT ... WINDOW ".to_string()));
    }
    if select.qualify.is_some() {
        return Err(CvsSqlError::Unsupported("SELECT ... QUALIFY".to_string()));
    }
    if select.value_table_mode.is_some() {
        return Err(CvsSqlError::Unsupported(
            "SELECT AS VALUE/STRUCT".to_string(),
        ));
    }
    if select.connect_by.is_some() {
        return Err(CvsSqlError::Unsupported(
            "SELECT ... CONNECT BY".to_string(),
        ));
    }

    if select.from.is_empty() {
        return Err(CvsSqlError::Unsupported("SELECT without FROM".to_string()));
    }

    let product = create_join(&select.from, engine)?;

    let filter = make_filter(engine, &select.selection, product)?;

    let mut group_by = if force_group {
        force_group_by(filter)
    } else {
        match &select.group_by {
            GroupByExpr::All(_) => {
                return Err(CvsSqlError::Unsupported(
                    "SELECT ... GROUP BY ALL".to_string(),
                ));
            }
            GroupByExpr::Expressions(exp, mods) => {
                if !mods.is_empty() {
                    return Err(CvsSqlError::Unsupported(
                        "SELECT ... GROUP BY WITH".to_string(),
                    ));
                }
                group_by(engine, exp, filter)?
            }
        }
    };
    apply_having(engine, &select.having, &mut group_by)?;

    order_by(engine, order, &mut group_by)?;
    trim(limit, offset, engine, &mut group_by)?;
    match make_projection(engine, group_by, &select.projection) {
        Ok(proj) => Ok(proj),
        Err(CvsSqlError::NoGroupBy) => {
            if !force_group {
                extract(select, order, limit, offset, engine, true)
            } else {
                Err(CvsSqlError::NoGroupBy)
            }
        }
        Err(e) => Err(e),
    }
}

impl Extractor for TableFactor {
    fn extract(&self, engine: &Engine) -> Result<ResultSet, CvsSqlError> {
        match self {
            TableFactor::Table {
                name,
                alias,
                args,
                with_hints,
                version,
                with_ordinality,
                partitions,
                json_path,
                sample,
                index_hints,
            } => {
                if args.is_some() {
                    return Err(CvsSqlError::Unsupported(
                        "SELECT ... FROM with table arguments".to_string(),
                    ));
                }
                if !with_hints.is_empty() {
                    return Err(CvsSqlError::Unsupported(
                        "SELECT ... FROM  WITH".to_string(),
                    ));
                }
                if version.is_some() {
                    return Err(CvsSqlError::Unsupported(
                        "SELECT ... FROM  with version".to_string(),
                    ));
                }
                if *with_ordinality {
                    return Err(CvsSqlError::Unsupported(
                        "SELECT ... FROM  WITH ORDINALITY".to_string(),
                    ));
                }
                if !partitions.is_empty() {
                    return Err(CvsSqlError::Unsupported(
                        "SELECT ... FROM with partition".to_string(),
                    ));
                }
                if json_path.is_some() {
                    return Err(CvsSqlError::Unsupported(
                        "SELECT ... FROM with JSON path".to_string(),
                    ));
                }
                if sample.is_some() {
                    return Err(CvsSqlError::Unsupported(
                        "SELECT ... FROM with sample".to_string(),
                    ));
                }
                if !index_hints.is_empty() {
                    return Err(CvsSqlError::Unsupported(
                        "SELECT ... FROM with index hints".to_string(),
                    ));
                }

                let results = read_file(engine, name)?;
                if let Some(alias) = alias {
                    if !alias.columns.is_empty() {
                        return Err(CvsSqlError::Unsupported(
                            "SELECT ... FROM with subquery column alias".to_string(),
                        ));
                    }
                    Ok(alias_results(&alias.name, results))
                } else {
                    Ok(results)
                }
            }
            TableFactor::Derived {
                lateral,
                subquery,
                alias,
            } => {
                if *lateral {
                    return Err(CvsSqlError::Unsupported(
                        "SELECT ... FROM with lateral subquery".to_string(),
                    ));
                }
                let results = subquery.extract(engine)?;
                if let Some(alias) = alias {
                    if !alias.columns.is_empty() {
                        return Err(CvsSqlError::Unsupported(
                            "SELECT ... FROM with subquery column alias".to_string(),
                        ));
                    }
                    Ok(alias_results(&alias.name, results))
                } else {
                    Ok(results)
                }
            }
            _ => Err(CvsSqlError::Unsupported(
                "SELECT ... FROM must be a table or sub query".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::{
        ast::{
            ConnectBy, FormatClause, Ident, JsonPath, Statement, TableAlias, TableAliasColumnDef,
            TableIndexHintType, TableIndexHints, TableIndexType, TableSample, TableSampleKind,
            TableSampleModifier, TableVersion, ValueTableMode,
        },
        parser::Parser,
    };

    use crate::{args::Args, dialect::FilesDialect};

    use super::*;

    fn test_unsupported_extractor(change: impl Fn(&mut Query)) -> Result<(), CvsSqlError> {
        let args = Args {
            writer_mode: true,
            ..Args::default()
        };

        let engine = Engine::try_from(&args)?;

        let sql = "SELECT * FROM tests.data.dates LIMIT 10";
        let dialect = FilesDialect {};
        let statement = Parser::parse_sql(&dialect, sql)?;
        let Some(Statement::Query(mut query)) = statement.into_iter().next() else {
            panic!("Not a select statement");
        };
        change(&mut query);

        let Err(err) = query.extract(&engine) else {
            panic!("No error");
        };

        assert!(matches!(err, CvsSqlError::Unsupported(_)));

        Ok(())
    }

    fn test_unsupported_select(change: impl Fn(&mut Select)) -> Result<(), CvsSqlError> {
        test_unsupported_extractor(|query| {
            let SetExpr::Select(ref mut select) = *query.body else {
                panic!("No select?");
            };
            change(select);
        })
    }

    fn test_unsupported_table_factor(change: impl Fn(&mut TableFactor)) -> Result<(), CvsSqlError> {
        test_unsupported_select(|select| {
            let Some(table_factor) = select.from.first() else {
                panic!("No table?");
            };
            let mut table_factor = table_factor.clone();
            change(&mut table_factor.relation);
            select.from = vec![table_factor];
        })
    }

    #[test]
    fn limit_by_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported_extractor(|query| {
            let Some(LimitClause::LimitOffset {
                limit,
                offset: _,
                limit_by: _,
            }) = &query.limit_clause
            else {
                panic!("No limit?")
            };
            let Some(limit) = limit else {
                panic!("No limit?")
            };
            query.limit_clause = Some(LimitClause::LimitOffset {
                limit: None,
                offset: None,
                limit_by: vec![limit.clone()],
            });
        })
    }

    #[test]
    fn settings_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported_extractor(|query| {
            query.settings = Some(vec![]);
        })
    }

    #[test]
    fn format_clause_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported_extractor(|query| {
            query.format_clause = Some(FormatClause::Null);
        })
    }

    #[test]
    fn prewhere_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported_select(|select| {
            let ext = Ident::new("test");
            select.prewhere = Some(Expr::Identifier(ext));
        })
    }

    #[test]
    fn value_table_mode_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported_select(|select| {
            select.value_table_mode = Some(ValueTableMode::AsStruct);
        })
    }

    #[test]
    fn connect_by_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported_select(|select| {
            let ext = Ident::new("test");
            select.connect_by = Some(ConnectBy {
                condition: Expr::Identifier(ext),
                relationships: vec![],
            });
        })
    }

    #[test]
    fn empty_from_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported_select(|select| select.from = vec![])
    }

    #[test]
    fn version_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported_table_factor(|from| {
            let ext = Expr::Identifier(Ident::new("test"));
            match from {
                TableFactor::Table {
                    name: _,
                    alias: _,
                    args: _,
                    with_hints: _,
                    version,
                    with_ordinality: _,
                    partitions: _,
                    json_path: _,
                    sample: _,
                    index_hints: _,
                } => *version = Some(TableVersion::Function(ext)),
                _ => {
                    panic!("Not a table?")
                }
            };
        })
    }

    #[test]
    fn with_ordinality_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported_table_factor(|from| {
            match from {
                TableFactor::Table {
                    name: _,
                    alias: _,
                    args: _,
                    with_hints: _,
                    version: _,
                    with_ordinality,
                    partitions: _,
                    json_path: _,
                    sample: _,
                    index_hints: _,
                } => *with_ordinality = true,
                _ => {
                    panic!("Not a table?")
                }
            };
        })
    }

    #[test]
    fn partitions_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported_table_factor(|from| {
            match from {
                TableFactor::Table {
                    name: _,
                    alias: _,
                    args: _,
                    with_hints: _,
                    version: _,
                    with_ordinality: _,
                    partitions,
                    json_path: _,
                    sample: _,
                    index_hints: _,
                } => *partitions = vec![Ident::from("test")],
                _ => {
                    panic!("Not a table?")
                }
            };
        })
    }

    #[test]
    fn json_path_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported_table_factor(|from| {
            match from {
                TableFactor::Table {
                    name: _,
                    alias: _,
                    args: _,
                    with_hints: _,
                    version: _,
                    with_ordinality: _,
                    partitions: _,
                    json_path,
                    sample: _,
                    index_hints: _,
                } => *json_path = Some(JsonPath { path: vec![] }),
                _ => {
                    panic!("Not a table?")
                }
            };
        })
    }

    #[test]
    fn sample_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported_table_factor(|from| {
            let table_sample = TableSample {
                modifier: TableSampleModifier::Sample,
                name: None,
                quantity: None,
                seed: None,
                bucket: None,
                offset: None,
            };
            match from {
                TableFactor::Table {
                    name: _,
                    alias: _,
                    args: _,
                    with_hints: _,
                    version: _,
                    with_ordinality: _,
                    partitions: _,
                    json_path: _,
                    sample,
                    index_hints: _,
                } => *sample = Some(TableSampleKind::BeforeTableAlias(Box::new(table_sample))),
                _ => {
                    panic!("Not a table?")
                }
            };
        })
    }

    #[test]
    fn index_hints_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported_table_factor(|from| {
            let hint = TableIndexHints {
                hint_type: TableIndexHintType::Use,
                index_type: TableIndexType::Key,
                for_clause: None,
                index_names: vec![],
            };
            match from {
                TableFactor::Table {
                    name: _,
                    alias: _,
                    args: _,
                    with_hints: _,
                    version: _,
                    with_ordinality: _,
                    partitions: _,
                    json_path: _,
                    sample: _,
                    index_hints,
                } => *index_hints = vec![hint],
                _ => {
                    panic!("Not a table?")
                }
            };
        })
    }

    #[test]
    fn sub_query_column_alias_unsupported() -> Result<(), CvsSqlError> {
        test_unsupported_table_factor(|from| {
            let defs = TableAliasColumnDef::from_name("name");
            match from {
                TableFactor::Table {
                    name: _,
                    alias,
                    args: _,
                    with_hints: _,
                    version: _,
                    with_ordinality: _,
                    partitions: _,
                    json_path: _,
                    sample: _,
                    index_hints: _,
                } => {
                    *alias = Some(TableAlias {
                        name: Ident::from("name"),
                        columns: vec![defs],
                    });
                }
                _ => {
                    panic!("Not a table?")
                }
            };
        })
    }
}
