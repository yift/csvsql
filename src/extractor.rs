use sqlparser::ast::{GroupByExpr, Query, Select, SetExpr, Statement, TableFactor};

use crate::cartesian_product_results::join;
use crate::error::CdvSqlError;
use crate::file_results::read_file;
use crate::named_results::alias_results;
use crate::projections::make_projection;
use crate::{engine::Engine, results::ResultSet};
pub trait Extractor {
    fn extract(&self, engine: &Engine) -> Result<Box<dyn ResultSet>, CdvSqlError>;
}

impl Extractor for Statement {
    fn extract(&self, engine: &Engine) -> Result<Box<dyn ResultSet>, CdvSqlError> {
        match self {
            Statement::Query(query) => query.extract(engine),
            _ => Err(CdvSqlError::Unsupported(self.to_string())),
        }
    }
}

impl Extractor for Query {
    fn extract(&self, engine: &Engine) -> Result<Box<dyn ResultSet>, CdvSqlError> {
        if self.fetch.is_some() {
            return Err(CdvSqlError::Unsupported("SELECT ... FETCH".to_string()));
        }
        if self.for_clause.is_some() {
            return Err(CdvSqlError::Unsupported("SELECT ... FOR".to_string()));
        }
        if self.with.is_some() {
            return Err(CdvSqlError::Unsupported("SELECT ... WITH".to_string()));
        }
        if !self.limit_by.is_empty() {
            return Err(CdvSqlError::Unsupported("SELECT ... LIMIT BY".to_string()));
        }
        if !self.locks.is_empty() {
            return Err(CdvSqlError::Unsupported(
                "SELECT ... FOR UPDATE/SHARE".to_string(),
            ));
        }

        if self.order_by.is_some() {
            return Err(CdvSqlError::ToDo("SELECT ... ORDER BY".to_string()));
        }
        if self.limit.is_some() {
            return Err(CdvSqlError::ToDo("SELECT ... LIMIT".to_string()));
        }
        if self.offset.is_some() {
            return Err(CdvSqlError::ToDo("SELECT ... OFFSET".to_string()));
        }
        if self.settings.is_some() {
            return Err(CdvSqlError::ToDo("SELECT ... SETTINGS".to_string()));
        }
        if self.format_clause.is_some() {
            return Err(CdvSqlError::ToDo("SELECT ... FORMAT".to_string()));
        }

        match &*self.body {
            SetExpr::Select(select) => select.extract(engine),
            SetExpr::Query(_) => Err(CdvSqlError::ToDo("SELECT (SELECT ...)".to_string())),
            SetExpr::Values(_) => Err(CdvSqlError::Unsupported("SELECT ... VALUES".to_string())),
            SetExpr::Insert(_) => Err(CdvSqlError::Unsupported("SELECT ... INSERT".to_string())),
            SetExpr::Table(_) => Err(CdvSqlError::Unsupported("SELECT ... TABLE".to_string())),
            SetExpr::Update(_) => Err(CdvSqlError::Unsupported("SELECT ... UPDATE".to_string())),
            SetExpr::SetOperation {
                op: _,
                set_quantifier: _,
                left: _,
                right: _,
            } => Err(CdvSqlError::Unsupported(
                "SELECT ... UNION/EXCEPT/INTERSECT".to_string(),
            )),
        }
    }
}

impl Extractor for Select {
    fn extract(&self, engine: &Engine) -> Result<Box<dyn ResultSet>, CdvSqlError> {
        if self.distinct.is_some() {
            return Err(CdvSqlError::Unsupported("SELECT DISTINCT".to_string()));
        }
        if self.top.is_some() {
            return Err(CdvSqlError::Unsupported("SELECT TOP".to_string()));
        }
        if self.top_before_distinct {
            return Err(CdvSqlError::Unsupported("SELECT ALL".to_string()));
        }
        if self.into.is_some() {
            return Err(CdvSqlError::Unsupported("SELECT INTO".to_string()));
        }
        if !self.lateral_views.is_empty() {
            return Err(CdvSqlError::Unsupported("SELECT LATERAL VIEW".to_string()));
        }
        if self.prewhere.is_some() {
            return Err(CdvSqlError::Unsupported("SELECT ... PREWHERE".to_string()));
        }
        if !self.cluster_by.is_empty() {
            return Err(CdvSqlError::Unsupported(
                "SELECT ... CLUSTER BY".to_string(),
            ));
        }
        if !self.distribute_by.is_empty() {
            return Err(CdvSqlError::Unsupported(
                "SELECT ... DISTRIBUTE BY".to_string(),
            ));
        }
        if !self.sort_by.is_empty() {
            return Err(CdvSqlError::Unsupported("SELECT ... SORT BY".to_string()));
        }
        if !self.named_window.is_empty() || self.window_before_qualify {
            return Err(CdvSqlError::Unsupported("SELECT ... WINDOW ".to_string()));
        }
        if self.qualify.is_some() {
            return Err(CdvSqlError::Unsupported("SELECT ... QUALIFY".to_string()));
        }
        if self.value_table_mode.is_some() {
            return Err(CdvSqlError::Unsupported(
                "SELECT AS VALUE/STRUCT".to_string(),
            ));
        }
        if self.connect_by.is_some() {
            return Err(CdvSqlError::Unsupported(
                "SELECT ... CONNECT BY".to_string(),
            ));
        }

        if self.selection.is_some() {
            return Err(CdvSqlError::ToDo("SELECT ... WHERE".to_string()));
        }
        if self.having.is_some() {
            return Err(CdvSqlError::ToDo("SELECT ... HAVING".to_string()));
        }
        match &self.group_by {
            GroupByExpr::All(_) => {
                return Err(CdvSqlError::Unsupported(
                    "SELECT ... GROUP BY ALL".to_string(),
                ))
            }
            GroupByExpr::Expressions(exp, mods) => {
                if !exp.is_empty() {
                    return Err(CdvSqlError::ToDo("SELECT ... GROUP BY".to_string()));
                }
                if !mods.is_empty() {
                    return Err(CdvSqlError::ToDo("SELECT ... GROUP BY".to_string()));
                }
            }
        }

        if self.from.is_empty() {
            return Err(CdvSqlError::Unsupported("SELECT without FROM".to_string()));
        }

        let mut product = None;

        for from in &self.from {
            if !from.joins.is_empty() {
                return Err(CdvSqlError::ToDo("SELECT ... JOIN".to_string()));
            }
            let from = from.relation.extract(engine)?;
            product = match product {
                None => Some(from),
                Some(left) => Some(join(left, from)),
            };
        }

        let Some(product) = product else {
            return Err(CdvSqlError::Unsupported("SELECT without FROM".to_string()));
        };

        make_projection(product, &self.projection)

        /*
        pub enum SelectItem {
            /// Any expression, not followed by `[ AS ] alias`
            UnnamedExpr(Expr),
            /// An expression, followed by `[ AS ] alias`
            ExprWithAlias { expr: Expr, alias: Ident },
            /// `alias.*` or even `schema.table.*`
            QualifiedWildcard(ObjectName, WildcardAdditionalOptions),
            /// An unqualified `*`
            Wildcard(WildcardAdditionalOptions),
        }

                /// projection expressions
                pub projection: Vec<SelectItem>,
                     */
    }
}

impl Extractor for TableFactor {
    fn extract(&self, engine: &Engine) -> Result<Box<dyn ResultSet>, CdvSqlError> {
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
            } => {
                if args.is_some() {
                    return Err(CdvSqlError::Unsupported(
                        "SELECT ... FROM with table arguments".to_string(),
                    ));
                }
                if !with_hints.is_empty() {
                    return Err(CdvSqlError::Unsupported(
                        "SELECT ... FROM  WITH".to_string(),
                    ));
                }
                if version.is_some() {
                    return Err(CdvSqlError::Unsupported(
                        "SELECT ... FROM  with version".to_string(),
                    ));
                }
                if *with_ordinality {
                    return Err(CdvSqlError::Unsupported(
                        "SELECT ... FROM  WITH ORDINALITY".to_string(),
                    ));
                }
                if !partitions.is_empty() {
                    return Err(CdvSqlError::Unsupported(
                        "SELECT ... FROM with partition".to_string(),
                    ));
                }
                if json_path.is_some() {
                    return Err(CdvSqlError::Unsupported(
                        "SELECT ... FROM with JSON path".to_string(),
                    ));
                }

                let results = read_file(&name.0, &engine.home, engine.first_line_as_name)?;
                if let Some(alias) = alias {
                    if !alias.columns.is_empty() {
                        return Err(CdvSqlError::Unsupported(
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
                    return Err(CdvSqlError::Unsupported(
                        "SELECT ... FROM with lateral subquery".to_string(),
                    ));
                }
                let results = subquery.extract(engine)?;
                if let Some(alias) = alias {
                    if !alias.columns.is_empty() {
                        return Err(CdvSqlError::Unsupported(
                            "SELECT ... FROM with subquery column alias".to_string(),
                        ));
                    }
                    Ok(alias_results(&alias.name, results))
                } else {
                    Ok(results)
                }
            }
            _ => Err(CdvSqlError::Unsupported(
                "SELECT ... FROM must be a table or sub query".to_string(),
            )),
        }
        /*match self {
        Table {
            #[cfg_attr(feature = "visitor", visit(with = "visit_relation"))]
            name: ObjectName,
            alias: Option<TableAlias>,
            /// Arguments of a table-valued function, as supported by Postgres
            /// and MSSQL. Note that deprecated MSSQL `FROM foo (NOLOCK)` syntax
            /// will also be parsed as `args`.
            ///
            /// This field's value is `Some(v)`, where `v` is a (possibly empty)
            /// vector of arguments, in the case of a table-valued function call,
            /// whereas it's `None` in the case of a regular table name.
            args: Option<TableFunctionArgs>,
            /// MSSQL-specific `WITH (...)` hints such as NOLOCK.
            with_hints: Vec<Expr>,
            /// Optional version qualifier to facilitate table time-travel, as
            /// supported by BigQuery and MSSQL.
            version: Option<TableVersion>,
            //  Optional table function modifier to generate the ordinality for column.
            /// For example, `SELECT * FROM generate_series(1, 10) WITH ORDINALITY AS t(a, b);`
            /// [WITH ORDINALITY](https://www.postgresql.org/docs/current/functions-srf.html), supported by Postgres.
            with_ordinality: bool,
            /// [Partition selection](https://dev.mysql.com/doc/refman/8.0/en/partitioning-selection.html), supported by MySQL.
            partitions: Vec<Ident>,
            /// Optional PartiQL JsonPath: <https://partiql.org/dql/from.html>
            json_path: Option<JsonPath>,

         }*/
    }
}
/*
struct NamedResults {
    results: Box<dyn ResultSet>,
    alias: String,
}
impl ResultSet for NamedResults {
    fn name(&self) -> &str {
        &self.alias
    }
    fn metadata(&self) -> &ResultSetMetaData {
        &self.results.metadata()
    }
    fn lines(&self) -> &[ResultLine] {
        self.results.lines()
    }
}

struct FileResults {
    name: String,
    metadata: ResultSetMetaData,
    lines: Vec<ResultLine>,
}
impl ResultSet for FileResults {
    fn name(&self) -> &str {
        &self.name
    }
    fn metadata(&self) -> &ResultSetMetaData {
        &self.metadata
    }
    fn lines(&self) -> &[ResultLine] {
        &self.lines
    }
}

impl FileResults {
    fn new(path: &PathBuf, first_line_as_name: bool) -> Result<Self, IoError> {
        let mut reader = ReaderBuilder::new()
            .flexible(true)
            .has_headers(first_line_as_name)
            .from_path(path)?;
        let mut cols = Vec::new();
        if first_line_as_name {
            let header = reader.headers()?;
            for h in header {
                cols.push(h.to_string());
            }
        }
        let mut lines = Vec::new();
        for records in reader.records() {
            let mut line = Vec::new();
            let records = records?;
            for (index, record) in records.iter().enumerate() {
                let value = Value::from(record);
                line.push(value);
                if index >= cols.len() {
                    cols.push(Self::get_default_header(index));
                }
            }
            lines.push(ResultLine::new(line));
        }

        let name = path
            .file_stem()
            .and_then(|f| f.to_str())
            .unwrap_or_default()
            .to_string();
        let metadata = ResultSetMetaData::new(cols);

        Ok(FileResults {
            name,
            metadata,
            lines,
        })
    }

    fn get_default_header(index: usize) -> String {
        let mut index = index;
        let mut title = String::new();
        let first = 'A' as usize;
        let size = 'Z' as usize - first + 1;
        loop {
            let chr = index % (size);
            index -= chr;
            title.insert(0, char::from((chr + first) as u8));
            if index == 0 {
                break;
            }
            index = index / size - 1;
        }
        title
    }
}

struct ProductResults {
    name: String,
    data: Vec<Box<dyn ResultSet>>,
}

impl ResultSet for ProductResults {
    fn name(&self) -> &str {
        &self.name
    }
    fn metadata(&self) -> &ResultSetMetaData {}
    fn lines(&self) -> &[ResultLine] {}
}
*/
