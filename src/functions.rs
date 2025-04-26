use std::{ops::Deref, str::FromStr};

use crate::{
    engine::Engine,
    error::CvsSqlError,
    group_by::GroupRow,
    projections::{Projection, SingleConvert},
    result_set_metadata::Metadata,
    util::SmartReference,
    value::Value,
};
use bigdecimal::FromPrimitive;
use bigdecimal::ToPrimitive;
use bigdecimal::{BigDecimal, Zero};
use chrono::{TimeZone, Utc, offset::LocalResult};
use itertools::Itertools;
use regex::Regex;
use sqlparser::ast::{
    DuplicateTreatment, Function, FunctionArg, FunctionArgExpr, FunctionArguments,
};

impl SingleConvert for Function {
    fn convert_single(
        &self,
        metadata: &Metadata,
        engine: &Engine,
    ) -> Result<Box<dyn Projection>, CvsSqlError> {
        if !self.within_group.is_empty() {
            return Err(CvsSqlError::Unsupported("WITHIN GROUP".into()));
        }

        if self.over.is_some() {
            return Err(CvsSqlError::Unsupported("OVER".into()));
        }

        if self.null_treatment.is_some() {
            return Err(CvsSqlError::Unsupported("IGNORE/RESPECT NULLS".into()));
        }

        if self.filter.is_some() {
            return Err(CvsSqlError::Unsupported("FILTER".into()));
        }
        if self.parameters != FunctionArguments::None {
            return Err(CvsSqlError::Unsupported("function parameters".into()));
        }

        let name = self.name.to_string().to_uppercase();
        build_function_from_name(&name, metadata, engine, &self.args)
    }
}
fn build_function_from_name(
    name: &str,
    metadata: &Metadata,
    engine: &Engine,
    args: &FunctionArguments,
) -> Result<Box<dyn Projection>, CvsSqlError> {
    match name {
        "COUNT" => build_aggregator_function(metadata, engine, args, Box::new(Count {})),
        "AVG" => build_aggregator_function(metadata, engine, args, Box::new(Avg {})),
        "SUM" => build_aggregator_function(metadata, engine, args, Box::new(Sum {})),
        "MIN" => build_aggregator_function(metadata, engine, args, Box::new(Min {})),
        "MAX" => build_aggregator_function(metadata, engine, args, Box::new(Max {})),
        "ANY_VALUE" => build_aggregator_function(metadata, engine, args, Box::new(AnyValue {})),

        "ABS" => build_function(metadata, engine, args, Box::new(Abs {})),
        "ASCII" => build_function(metadata, engine, args, Box::new(Ascii {})),
        "CHR" => build_function(metadata, engine, args, Box::new(Chr {})),
        "LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH" => {
            build_function(metadata, engine, args, Box::new(Length {}))
        }
        "COALESCE" => build_function(metadata, engine, args, Box::new(Coalece {})),
        "CONCAT" => build_function(metadata, engine, args, Box::new(Concat {})),
        "CONCAT_WS" => build_function(metadata, engine, args, Box::new(ConcatWs {})),
        "CURRENT_DATE" | "CURDATE" => {
            build_function(metadata, engine, args, Box::new(CurrentDate {}))
        }
        "NOW" | "CURRENT_TIME" | "CURRENT_TIMESTAMP" | "CURTIME" | "LOCALTIME"
        | "LOCALTIMESTAMP" => build_function(metadata, engine, args, Box::new(Now {})),
        "USER" | "CURRENT_USER" => build_function(metadata, engine, args, Box::new(User {})),
        "FORMAT" | "DATE_FORMAT" | "TIME_FORMAT" | "TO_CHAR" => {
            build_function(metadata, engine, args, Box::new(Format {}))
        }
        "TO_TIMESTAMP" | "FROM_UNIXTIME" => {
            build_function(metadata, engine, args, Box::new(ToTimestamp {}))
        }
        "GREATEST" => build_function(metadata, engine, args, Box::new(Greatest {})),
        "IF" => build_function(metadata, engine, args, Box::new(If {})),
        "NULLIF" => build_function(metadata, engine, args, Box::new(NullIf {})),
        "LOWER" | "LCASE" => build_function(metadata, engine, args, Box::new(Lower {})),
        "UPPER" | "UCASE" => build_function(metadata, engine, args, Box::new(Upper {})),
        "LEAST" => build_function(metadata, engine, args, Box::new(Least {})),
        "LEFT" => build_function(metadata, engine, args, Box::new(Left {})),
        "RIGHT" => build_function(metadata, engine, args, Box::new(Right {})),
        "LPAD" => build_function(metadata, engine, args, Box::new(Lpad {})),
        "RPAD" => build_function(metadata, engine, args, Box::new(Rpad {})),
        "LTRIM" => build_function(metadata, engine, args, Box::new(Ltrim {})),
        "RTRIM" => build_function(metadata, engine, args, Box::new(Rtrim {})),
        "PI" => build_function(metadata, engine, args, Box::new(Pi {})),
        "RANDOM" | "RAND" => build_function(metadata, engine, args, Box::new(Random {})),
        "POSITION" | "LOCATE" => build_function(metadata, engine, args, Box::new(Position {})),
        "REPEAT" => build_function(metadata, engine, args, Box::new(Repeat {})),
        "REPLACE" => build_function(metadata, engine, args, Box::new(Replace {})),
        "REGEX_LIKE" => build_function(metadata, engine, args, Box::new(RegexLike {})),
        "REGEX_REPLACE" => build_function(metadata, engine, args, Box::new(RegexReplace {})),
        "REGEXP_SUBSTR" => build_function(metadata, engine, args, Box::new(RegexSubstring {})),
        "REVERSE" => build_function(metadata, engine, args, Box::new(Reverse {})),
        "LN" => build_function(metadata, engine, args, Box::new(Ln {})),
        "EXP" => build_function(metadata, engine, args, Box::new(Exp {})),
        "LOG" => build_function(metadata, engine, args, Box::new(Log {})),
        "LOG2" => build_function(metadata, engine, args, Box::new(Log2 {})),
        "LOG10" => build_function(metadata, engine, args, Box::new(Log10 {})),
        "POW" | "POWER" => build_function(metadata, engine, args, Box::new(Power {})),
        "ROUND" => build_function(metadata, engine, args, Box::new(Round {})),
        "SQRT" => build_function(metadata, engine, args, Box::new(Sqrt {})),
        _ => Err(CvsSqlError::Unsupported(format!("function {}", name))),
    }
}

fn build_aggregator_function(
    metadata: &Metadata,
    engine: &Engine,
    args: &FunctionArguments,
    operator: Box<dyn AggregateOperator>,
) -> Result<Box<dyn Projection>, CvsSqlError> {
    let parent_metadata = match metadata {
        Metadata::Grouped { parent, this: _ } => parent,
        _ => return Err(CvsSqlError::NoGroupBy),
    };
    let lst = match &args {
        FunctionArguments::List(lst) => lst,
        FunctionArguments::Subquery(_) => {
            return Err(CvsSqlError::Unsupported(
                "function subquery arguments".into(),
            ));
        }
        FunctionArguments::None => {
            return Err(CvsSqlError::Unsupported(format!(
                "Function {} must have an argmeunt",
                operator.name()
            )));
        }
    };
    let distinct = matches!(lst.duplicate_treatment, Some(DuplicateTreatment::Distinct));

    if let Some(c) = lst.clauses.first() {
        return Err(CvsSqlError::Unsupported(format!("{}", c)));
    }
    let first = match lst.args.first() {
        Some(arg) => arg,
        None => {
            return Err(CvsSqlError::Unsupported(format!(
                "Function {} must have an argmeunt",
                operator.name()
            )));
        }
    };
    if lst.args.len() > 1 {
        return Err(CvsSqlError::Unsupported(format!(
            "Function {} must have a single argmeunt",
            operator.name()
        )));
    }
    let argument = match first {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
            e.convert_single(parent_metadata, engine)?
        }
        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
            if operator.support_wildcard_argument() {
                if distinct {
                    return Err(CvsSqlError::Unsupported("DISTINCT with * argument".into()));
                }
                wildcard_operator()
            } else {
                return Err(CvsSqlError::Unsupported(format!(
                    "Function {} with * argument",
                    operator.name()
                )));
            }
        }
        _ => return Err(CvsSqlError::Unsupported(format!("{}", first))),
    };
    let name = format!("{}({})", operator.name(), argument.name());

    Ok(Box::new(AggregatedFunction {
        distinct,
        argument,
        operator,
        name,
    }))
}

#[cfg(test)]
struct AggregationExample<'a> {
    name: &'a str,
    data: Vec<&'a str>,
    is_wildcard: bool,
    is_distinct: bool,
    expected_results: &'a str,
}
trait AggregateOperator {
    fn name(&self) -> &str;
    fn support_wildcard_argument(&self) -> bool;
    fn aggreagate(&self, data: &mut dyn Iterator<Item = Value>) -> Value;
    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<AggregationExample<'a>>;
}

trait Casters {
    fn to_number(self) -> Option<BigDecimal>;
}

impl Casters for Value {
    fn to_number(self) -> Option<BigDecimal> {
        match self {
            Value::Number(num) => Some(num),
            _ => None,
        }
    }
}

struct Count {}

impl AggregateOperator for Count {
    fn name(&self) -> &str {
        "COUNT"
    }
    fn support_wildcard_argument(&self) -> bool {
        true
    }
    fn aggreagate(&self, data: &mut dyn Iterator<Item = Value>) -> Value {
        let count = data.count();
        Value::Number((count as u128).into())
    }
    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<AggregationExample<'a>> {
        vec![
            AggregationExample {
                name: "simple",
                data: vec!["1", "2", "3", "4", "1"],
                is_distinct: false,
                is_wildcard: false,
                expected_results: "5",
            },
            AggregationExample {
                name: "wildcard",
                data: vec!["1", "2", "3", "4", "1"],
                is_distinct: false,
                is_wildcard: true,
                expected_results: "5",
            },
            AggregationExample {
                name: "distinct",
                data: vec!["1", "2", "3", "4", "1"],
                is_distinct: true,
                is_wildcard: false,
                expected_results: "4",
            },
        ]
    }
}

struct Avg {}

impl AggregateOperator for Avg {
    fn name(&self) -> &str {
        "AVG"
    }

    fn support_wildcard_argument(&self) -> bool {
        false
    }
    fn aggreagate(&self, data: &mut dyn Iterator<Item = Value>) -> Value {
        let mut total = BigDecimal::zero();
        let mut count: u128 = 0;
        for num in data.filter_map(|f| f.to_number()) {
            count += 1;
            total += num;
        }
        if count == 0 {
            Value::Empty
        } else {
            Value::Number(total / count)
        }
    }

    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<AggregationExample<'a>> {
        vec![
            AggregationExample {
                name: "simple",
                is_distinct: false,
                is_wildcard: false,
                data: vec!["5", "11", "11", "1"],
                expected_results: "7",
            },
            AggregationExample {
                name: "distinct",
                is_distinct: true,
                is_wildcard: false,
                data: vec!["30", "12", "12", "9"],
                expected_results: "17",
            },
            AggregationExample {
                name: "not_only_numbers",
                is_distinct: true,
                is_wildcard: false,
                data: vec!["10", "", "nop", "12"],
                expected_results: "11",
            },
            AggregationExample {
                name: "no_numbers",
                is_distinct: true,
                is_wildcard: false,
                data: vec!["a", "", "nop", ""],
                expected_results: "",
            },
        ]
    }
}

struct Sum {}

impl AggregateOperator for Sum {
    fn name(&self) -> &str {
        "SUM"
    }
    fn support_wildcard_argument(&self) -> bool {
        false
    }
    fn aggreagate(&self, data: &mut dyn Iterator<Item = Value>) -> Value {
        let total = data
            .filter_map(|f| f.to_number())
            .fold(BigDecimal::zero(), |a, b| a + b);
        Value::Number(total)
    }
    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<AggregationExample<'a>> {
        vec![
            AggregationExample {
                name: "simple",
                is_distinct: false,
                is_wildcard: false,
                data: vec!["1", "1", "2", "3"],
                expected_results: "7",
            },
            AggregationExample {
                name: "distinct",
                is_distinct: true,
                is_wildcard: false,
                data: vec!["1", "1", "2", "3"],
                expected_results: "6",
            },
            AggregationExample {
                name: "not_only_numbers",
                is_distinct: true,
                is_wildcard: false,
                data: vec!["10", "", "nop", "12"],
                expected_results: "22",
            },
            AggregationExample {
                name: "no_numbers",
                is_distinct: true,
                is_wildcard: false,
                data: vec!["a", "", "nop", ""],
                expected_results: "0",
            },
        ]
    }
}
struct Min {}

impl AggregateOperator for Min {
    fn name(&self) -> &str {
        "MIN"
    }
    fn support_wildcard_argument(&self) -> bool {
        false
    }
    fn aggreagate(&self, data: &mut dyn Iterator<Item = Value>) -> Value {
        let min = data.min();
        min.unwrap_or(Value::Empty)
    }

    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<AggregationExample<'a>> {
        vec![
            AggregationExample {
                name: "numbers",
                is_distinct: false,
                is_wildcard: false,
                data: vec!["1", "1", "2", "3"],
                expected_results: "1",
            },
            AggregationExample {
                name: "letters",
                is_distinct: true,
                is_wildcard: false,
                data: vec!["e", "b", "d", "q"],
                expected_results: "b",
            },
        ]
    }
}

struct Max {}
impl AggregateOperator for Max {
    fn name(&self) -> &str {
        "MAX"
    }
    fn support_wildcard_argument(&self) -> bool {
        false
    }
    fn aggreagate(&self, data: &mut dyn Iterator<Item = Value>) -> Value {
        let min = data.max();
        min.unwrap_or(Value::Empty)
    }
    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<AggregationExample<'a>> {
        vec![
            AggregationExample {
                name: "numbers",
                is_distinct: false,
                is_wildcard: false,
                data: vec!["1", "1", "2", "3"],
                expected_results: "3",
            },
            AggregationExample {
                name: "letters",
                is_distinct: true,
                is_wildcard: false,
                data: vec!["e", "b", "d", "q"],
                expected_results: "q",
            },
        ]
    }
}

struct AnyValue {}
impl AggregateOperator for AnyValue {
    fn name(&self) -> &str {
        "ANY_VALUE"
    }
    fn support_wildcard_argument(&self) -> bool {
        false
    }
    fn aggreagate(&self, data: &mut dyn Iterator<Item = Value>) -> Value {
        let val = data.next();
        val.unwrap_or(Value::Empty)
    }
    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<AggregationExample<'a>> {
        vec![AggregationExample {
            name: "values",
            is_distinct: false,
            is_wildcard: false,
            data: vec!["a", "b", "2", "3"],
            expected_results: "a",
        }]
    }
}

struct AggregatedFunction {
    distinct: bool,
    argument: Box<dyn Projection>,
    operator: Box<dyn AggregateOperator>,
    name: String,
}

impl Projection for AggregatedFunction {
    fn get<'a>(&'a self, row: &'a GroupRow) -> SmartReference<'a, Value> {
        let mut iter = row
            .group_rows
            .iter()
            .map(|r| self.argument.get(r))
            .map(|v| v.clone());
        let value = if self.distinct {
            let mut unique = iter.unique();
            self.operator.aggreagate(&mut unique)
        } else {
            self.operator.aggreagate(&mut iter)
        };
        value.into()
    }
    fn name(&self) -> &str {
        &self.name
    }
}

struct Wildcard {}
impl Projection for Wildcard {
    fn get<'a>(&'a self, _: &'a GroupRow) -> SmartReference<'a, Value> {
        Value::Bool(true).into()
    }
    fn name(&self) -> &str {
        "*"
    }
}
fn wildcard_operator() -> Box<dyn Projection> {
    Box::new(Wildcard {})
}

#[cfg(test)]
mod test_aggregations {
    use std::fs::{self, OpenOptions};

    use crate::{args::Args, engine::Engine, error::CvsSqlError, results::Column};
    use std::io::Write;

    use super::{AggregateOperator, AggregationExample, AnyValue, Avg, Count, Max, Min, Sum};

    fn test_agg(operator: &impl AggregateOperator) -> Result<(), CvsSqlError> {
        let dir = format!("./target/function_tests/{}", operator.name().to_lowercase());
        println!("testing: {}", operator.name());
        fs::remove_dir_all(&dir).ok();
        fs::create_dir_all(&dir)?;
        for example in operator.examples() {
            test_agg_with_example(operator, &example)?;
        }
        fs::remove_dir_all(&dir).ok();

        Ok(())
    }

    fn test_agg_with_example<'a>(
        operator: &impl AggregateOperator,
        example: &AggregationExample<'a>,
    ) -> Result<(), CvsSqlError> {
        println!("testing: {} with {}", operator.name(), example.name);
        let dir = format!("./target/function_tests/{}", operator.name().to_lowercase());
        let file = format!("{}/{}.csv", dir, example.name);
        let mut writer = OpenOptions::new().write(true).create(true).open(&file)?;
        writeln!(writer, "row")?;
        for data in &example.data {
            writeln!(writer, "{}", data)?;
        }

        let table_name = format!(
            "target.function_tests.{}.{}",
            operator.name().to_lowercase(),
            &example.name
        );
        let selector = if example.is_distinct {
            "DISTINCT row"
        } else if example.is_wildcard {
            "*"
        } else {
            "row"
        };

        let sql = format!(
            "SELECT {}({}) FROM {}\n",
            operator.name(),
            selector,
            table_name
        );

        let args = Args::default();
        let engine = Engine::try_from(&args)?;

        let results = engine.execute_commands(&sql)?;

        fs::remove_file(file)?;

        let col = Column::from_index(0);
        let result = results
            .first()
            .and_then(|d| d.data.iter().next())
            .map(|d| d.get(&col));
        let expected_results = example.expected_results.into();
        assert_eq!(result, Some(&expected_results));

        Ok(())
    }

    #[test]
    fn test_count() -> Result<(), CvsSqlError> {
        test_agg(&Count {})
    }

    #[test]
    fn test_sum() -> Result<(), CvsSqlError> {
        test_agg(&Sum {})
    }

    #[test]
    fn test_avg() -> Result<(), CvsSqlError> {
        test_agg(&Avg {})
    }

    #[test]
    fn test_min() -> Result<(), CvsSqlError> {
        test_agg(&Min {})
    }

    #[test]
    fn test_max() -> Result<(), CvsSqlError> {
        test_agg(&Max {})
    }

    #[test]
    fn test_any_value() -> Result<(), CvsSqlError> {
        test_agg(&AnyValue {})
    }
}

#[cfg(test)]
struct FunctionExample<'a> {
    name: &'a str,
    arguments: Vec<&'a str>,
    expected_results: &'a str,
}

trait Operator {
    fn name(&self) -> &str;
    fn min_args(&self) -> usize;
    fn max_args(&self) -> Option<usize>;
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value>;
    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<FunctionExample<'a>> {
        vec![]
    }
}
struct SimpleFunction {
    arguments: Vec<Box<dyn Projection>>,
    operator: Box<dyn Operator>,
    name: String,
}
impl Projection for SimpleFunction {
    fn get<'a>(&'a self, row: &'a GroupRow) -> SmartReference<'a, Value> {
        let mut args = vec![];
        for a in &self.arguments {
            args.push(a.get(row));
        }
        self.operator.get(&args)
    }
    fn name(&self) -> &str {
        &self.name
    }
}

fn build_function(
    metadata: &Metadata,
    engine: &Engine,
    args: &FunctionArguments,
    operator: Box<dyn Operator>,
) -> Result<Box<dyn Projection>, CvsSqlError> {
    let arguments = match &args {
        FunctionArguments::List(lst) => {
            if matches!(lst.duplicate_treatment, Some(DuplicateTreatment::Distinct)) {
                return Err(CvsSqlError::Unsupported(format!(
                    "Function {} with distinct argument",
                    operator.name()
                )));
            }
            if let Some(c) = lst.clauses.first() {
                return Err(CvsSqlError::Unsupported(format!("{}", c)));
            }
            let mut args = vec![];
            for a in &lst.args {
                let a = match a {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
                        e.convert_single(metadata, engine)?
                    }
                    _ => {
                        return Err(CvsSqlError::Unsupported(format!(
                            "{} as argment in function {}",
                            a,
                            operator.name()
                        )));
                    }
                };
                args.push(a);
            }
            args
        }
        FunctionArguments::Subquery(_) => {
            return Err(CvsSqlError::Unsupported(
                "function subquery arguments".into(),
            ));
        }
        FunctionArguments::None => vec![],
    };
    if arguments.len() < operator.min_args() {
        return Err(CvsSqlError::Unsupported(format!(
            "Function {} with {} argumnets or less",
            operator.name(),
            arguments.len()
        )));
    }
    if let Some(max) = operator.max_args() {
        if arguments.len() > max {
            return Err(CvsSqlError::Unsupported(format!(
                "Function {} with {} argumnets or more",
                operator.name(),
                arguments.len()
            )));
        }
    }
    let name = format!(
        "{}({})",
        operator.name(),
        arguments.iter().map(|f| f.name()).join(", ")
    );

    Ok(Box::new(SimpleFunction {
        arguments,
        operator,
        name,
    }))
}
impl From<Option<BigDecimal>> for SmartReference<'_, Value> {
    fn from(val: Option<BigDecimal>) -> Self {
        match val {
            None => Value::Empty,
            Some(num) => Value::Number(num),
        }
        .into()
    }
}

impl From<Option<u32>> for SmartReference<'_, Value> {
    fn from(val: Option<u32>) -> Self {
        match val {
            None => Value::Empty,
            Some(num) => Value::Number(num.into()),
        }
        .into()
    }
}
impl From<f64> for SmartReference<'_, Value> {
    fn from(val: f64) -> Self {
        BigDecimal::from_f64(val).into()
    }
}

impl From<Option<usize>> for SmartReference<'_, Value> {
    fn from(val: Option<usize>) -> Self {
        match val {
            None => Value::Empty,
            Some(num) => match BigDecimal::from_usize(num) {
                Some(num) => Value::Number(num),
                None => Value::Empty,
            },
        }
        .into()
    }
}
impl From<usize> for SmartReference<'_, Value> {
    fn from(val: usize) -> Self {
        match BigDecimal::from_usize(val) {
            Some(num) => Value::Number(num),
            None => Value::Empty,
        }
        .into()
    }
}

impl From<Option<String>> for SmartReference<'_, Value> {
    fn from(val: Option<String>) -> Self {
        match val {
            None => Value::Empty,
            Some(str) => Value::Str(str),
        }
        .into()
    }
}
impl From<Option<&str>> for SmartReference<'_, Value> {
    fn from(val: Option<&str>) -> Self {
        match val {
            None => Value::Empty,
            Some(str) => Value::Str(str.to_string()),
        }
        .into()
    }
}

trait Extractor {
    fn as_num(&self) -> Option<&BigDecimal>;
    fn as_string(&self) -> Option<&str>;
    fn as_bool(&self) -> Option<&bool>;
    fn as_u32(&self) -> Option<u32> {
        self.as_num().and_then(|s| s.to_u32())
    }
    fn as_i64(&self) -> Option<i64> {
        self.as_num().and_then(|s| s.to_i64())
    }
    fn as_usize(&self) -> Option<usize> {
        self.as_num().and_then(|s| s.to_usize())
    }
    fn as_f64(&self) -> Option<f64> {
        self.as_num().and_then(|s| s.to_f64())
    }
}
impl Extractor for Value {
    fn as_num(&self) -> Option<&BigDecimal> {
        match self {
            Value::Number(num) => Some(num),
            _ => None,
        }
    }
    fn as_string(&self) -> Option<&str> {
        match self {
            Value::Str(str) => Some(str),
            _ => None,
        }
    }
    fn as_bool(&self) -> Option<&bool> {
        match self {
            Value::Bool(b) => Some(b),
            _ => None,
        }
    }
}
impl<T: Extractor> Extractor for SmartReference<'_, T> {
    fn as_num(&self) -> Option<&BigDecimal> {
        self.deref().as_num()
    }
    fn as_string(&self) -> Option<&str> {
        self.deref().as_string()
    }
    fn as_bool(&self) -> Option<&bool> {
        self.deref().as_bool()
    }
}
impl<T: Extractor> Extractor for Option<&T> {
    fn as_num(&self) -> Option<&BigDecimal> {
        self.and_then(|t| t.as_num())
    }
    fn as_string(&self) -> Option<&str> {
        self.and_then(|s| s.as_string())
    }
    fn as_bool(&self) -> Option<&bool> {
        self.and_then(|s| s.as_bool())
    }
}

struct Abs {}
impl Operator for Abs {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        args.first().as_num().map(|t| t.abs()).into()
    }
    fn max_args(&self) -> Option<usize> {
        Some(1)
    }
    fn min_args(&self) -> usize {
        1
    }
    fn name(&self) -> &str {
        "ABS"
    }

    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<FunctionExample<'a>> {
        vec![
            FunctionExample {
                name: "positive_number",
                arguments: vec!["11.44"],
                expected_results: "11.44",
            },
            FunctionExample {
                name: "negative_number",
                arguments: vec!["-0.44"],
                expected_results: "0.44",
            },
            FunctionExample {
                name: "nan",
                arguments: vec!["test"],
                expected_results: "",
            },
        ]
    }
}

struct Ascii {}
impl Operator for Ascii {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        args.first()
            .as_string()
            .and_then(|s| s.chars().next())
            .map(|i| i as u32)
            .into()
    }
    fn max_args(&self) -> Option<usize> {
        Some(1)
    }
    fn min_args(&self) -> usize {
        1
    }
    fn name(&self) -> &str {
        "ASCII"
    }

    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<FunctionExample<'a>> {
        vec![
            FunctionExample {
                name: "simple",
                arguments: vec!["a"],
                expected_results: "97",
            },
            FunctionExample {
                name: "word",
                arguments: vec!["abc"],
                expected_results: "97",
            },
            FunctionExample {
                name: "number",
                arguments: vec!["100"],
                expected_results: "",
            },
        ]
    }
}

struct Chr {}
impl Operator for Chr {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        args.first()
            .as_u32()
            .and_then(char::from_u32)
            .map(|c| c.to_string())
            .into()
    }
    fn max_args(&self) -> Option<usize> {
        Some(1)
    }
    fn min_args(&self) -> usize {
        1
    }
    fn name(&self) -> &str {
        "CHR"
    }

    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<FunctionExample<'a>> {
        vec![
            FunctionExample {
                name: "simple",
                arguments: vec!["97"],
                expected_results: "a",
            },
            FunctionExample {
                name: "neg",
                arguments: vec!["-100"],
                expected_results: "",
            },
            FunctionExample {
                name: "float",
                arguments: vec!["97.1"],
                expected_results: "a",
            },
            FunctionExample {
                name: "str",
                arguments: vec!["abc"],
                expected_results: "",
            },
        ]
    }
}

struct Length {}
impl Operator for Length {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        args.first().as_string().map(|s| s.len()).into()
    }
    fn max_args(&self) -> Option<usize> {
        Some(1)
    }
    fn min_args(&self) -> usize {
        1
    }
    fn name(&self) -> &str {
        "LENGTH"
    }

    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<FunctionExample<'a>> {
        vec![
            FunctionExample {
                name: "simple",
                arguments: vec!["hello"],
                expected_results: "5",
            },
            FunctionExample {
                name: "number",
                arguments: vec!["-100"],
                expected_results: "",
            },
        ]
    }
}

struct Coalece {}
impl Operator for Coalece {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        for a in args.iter() {
            if !a.is_empty() {
                return a.deref().clone().into();
            }
        }
        Value::Empty.into()
    }
    fn max_args(&self) -> Option<usize> {
        None
    }
    fn min_args(&self) -> usize {
        0
    }
    fn name(&self) -> &str {
        "COALESCE"
    }

    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<FunctionExample<'a>> {
        vec![
            FunctionExample {
                name: "simple",
                arguments: vec!["", "", "5", "6"],
                expected_results: "5",
            },
            FunctionExample {
                name: "nope",
                arguments: vec!["", "", "", "", ""],
                expected_results: "",
            },
            FunctionExample {
                name: "first",
                arguments: vec!["a", "b"],
                expected_results: "a",
            },
            FunctionExample {
                name: "empty",
                arguments: vec![],
                expected_results: "",
            },
        ]
    }
}

struct Concat {}
impl Operator for Concat {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let str = args.iter().map(|f| f.to_string()).join("");
        Value::Str(str).into()
    }
    fn max_args(&self) -> Option<usize> {
        None
    }
    fn min_args(&self) -> usize {
        0
    }
    fn name(&self) -> &str {
        "CONCAT"
    }

    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<FunctionExample<'a>> {
        vec![
            FunctionExample {
                name: "simple",
                arguments: vec!["a", "b", "cd", "e"],
                expected_results: "abcde",
            },
            FunctionExample {
                name: "with_nums",
                arguments: vec!["a", "1", "b"],
                expected_results: "a1b",
            },
        ]
    }
}

struct ConcatWs {}
impl Operator for ConcatWs {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let Some(sep) = args.first() else {
            return Value::Empty.into();
        };
        let sep = sep.to_string();
        let str = args
            .iter()
            .skip(1)
            .filter(|f| !f.is_empty())
            .map(|f| f.to_string())
            .join(sep.as_str());
        Value::Str(str).into()
    }
    fn max_args(&self) -> Option<usize> {
        None
    }
    fn min_args(&self) -> usize {
        1
    }
    fn name(&self) -> &str {
        "CONCAT_WS"
    }

    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<FunctionExample<'a>> {
        vec![FunctionExample {
            name: "simple",
            arguments: vec!["|", "a", "b", "cd", "e"],
            expected_results: "a|b|cd|e",
        }]
    }
}

struct CurrentDate {}
impl Operator for CurrentDate {
    fn get<'a>(&'a self, _: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        Value::Date(Utc::now().naive_utc().date()).into()
    }
    fn max_args(&self) -> Option<usize> {
        Some(0)
    }
    fn min_args(&self) -> usize {
        0
    }
    fn name(&self) -> &str {
        "CURRENT_DATE"
    }
}
struct Now {}
impl Operator for Now {
    fn get<'a>(&'a self, _: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        Value::Timestamp(Utc::now().naive_utc()).into()
    }
    fn max_args(&self) -> Option<usize> {
        Some(0)
    }
    fn min_args(&self) -> usize {
        0
    }
    fn name(&self) -> &str {
        "NOW"
    }
}
struct User {}
impl Operator for User {
    fn get<'a>(&'a self, _: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        Value::Str(whoami::username()).into()
    }
    fn max_args(&self) -> Option<usize> {
        Some(0)
    }
    fn min_args(&self) -> usize {
        0
    }
    fn name(&self) -> &str {
        "CURRENT_USER"
    }
}

struct Format {}
impl Operator for Format {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let Some(value) = args.first() else {
            return Value::Empty.into();
        };
        let format = args.get(1);
        let Some(format) = format.as_string() else {
            return Value::Empty.into();
        };
        let formatted = match value.deref() {
            Value::Date(date) => date.format(format),
            Value::Timestamp(ts) => ts.format(format),
            _ => {
                return Value::Empty.into();
            }
        };
        let mut text = String::new();
        if formatted.write_to(&mut text).is_err() {
            return Value::Empty.into();
        }

        Value::Str(text).into()
    }
    fn max_args(&self) -> Option<usize> {
        Some(2)
    }
    fn min_args(&self) -> usize {
        2
    }
    fn name(&self) -> &str {
        "FORMAT"
    }

    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<FunctionExample<'a>> {
        vec![
            FunctionExample {
                name: "simple_date",
                arguments: vec!["2024-11-23", "%d/%m/%Y"],
                expected_results: "23/11/2024",
            },
            FunctionExample {
                name: "simple_timestmp",
                arguments: vec!["2024-11-23 16:20:21.003", "%v %r"],
                expected_results: "23-Nov-2024 04:20:21 PM",
            },
            FunctionExample {
                name: "format_as_number",
                arguments: vec!["2024-11-23 16:20:21.003", "123"],
                expected_results: "",
            },
            FunctionExample {
                name: "invalid_format",
                arguments: vec!["2024-11-23 16:20:21.003", "%Q"],
                expected_results: "",
            },
            FunctionExample {
                name: "numeric_value",
                arguments: vec!["3", "%v %r"],
                expected_results: "",
            },
        ]
    }
}

struct ToTimestamp {}
impl Operator for ToTimestamp {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let Some(time) = args.first().as_i64() else {
            return Value::Empty.into();
        };

        let LocalResult::Single(time) = Utc.timestamp_opt(time, 0) else {
            return Value::Empty.into();
        };

        Value::Timestamp(time.naive_utc()).into()
    }
    fn max_args(&self) -> Option<usize> {
        Some(1)
    }
    fn min_args(&self) -> usize {
        1
    }
    fn name(&self) -> &str {
        "TO_TIMESTAMP"
    }

    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<FunctionExample<'a>> {
        vec![
            FunctionExample {
                name: "should_work",
                arguments: vec!["1400234525"],
                expected_results: "2014-05-16 10:02:05",
            },
            FunctionExample {
                name: "nan",
                arguments: vec!["test"],
                expected_results: "",
            },
        ]
    }
}

struct Greatest {}
impl Operator for Greatest {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let mut greatest = None;
        for a in args.iter() {
            match &greatest {
                None => greatest = Some(a.deref().clone()),
                Some(val) => {
                    if a.deref() > val {
                        greatest = Some(a.deref().clone());
                    }
                }
            }
        }
        match greatest {
            None => Value::Empty.into(),
            Some(greatest) => greatest.into(),
        }
    }
    fn max_args(&self) -> Option<usize> {
        None
    }
    fn min_args(&self) -> usize {
        0
    }
    fn name(&self) -> &str {
        "GREATEST"
    }

    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<FunctionExample<'a>> {
        vec![
            FunctionExample {
                name: "greatest",
                arguments: vec!["10", "400040", "1044", "-134522352"],
                expected_results: "400040",
            },
            FunctionExample {
                name: "empty",
                arguments: vec![],
                expected_results: "",
            },
        ]
    }
}
struct If {}
impl Operator for If {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let first = args.first();
        let Some(condition) = first.as_bool() else {
            return Value::Empty.into();
        };
        let value = if *condition { args.get(1) } else { args.get(2) };
        match value {
            Some(v) => v.deref().clone().into(),
            None => Value::Empty.into(),
        }
    }
    fn max_args(&self) -> Option<usize> {
        Some(3)
    }
    fn min_args(&self) -> usize {
        3
    }
    fn name(&self) -> &str {
        "IF"
    }

    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<FunctionExample<'a>> {
        vec![
            FunctionExample {
                name: "true",
                arguments: vec!["TRUE", "100", "-100"],
                expected_results: "100",
            },
            FunctionExample {
                name: "false",
                arguments: vec!["FALSE", "100", "-100"],
                expected_results: "-100",
            },
            FunctionExample {
                name: "not_bool",
                arguments: vec!["test", "100", "-100"],
                expected_results: "",
            },
        ]
    }
}

struct NullIf {}
impl Operator for NullIf {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let Some(value_one) = args.first() else {
            return Value::Empty.into();
        };
        let Some(value_two) = args.get(1) else {
            return Value::Empty.into();
        };
        if *value_one != *value_two {
            value_one.deref().clone().into()
        } else {
            Value::Empty.into()
        }
    }
    fn max_args(&self) -> Option<usize> {
        Some(2)
    }
    fn min_args(&self) -> usize {
        2
    }
    fn name(&self) -> &str {
        "NULLIF"
    }

    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<FunctionExample<'a>> {
        vec![
            FunctionExample {
                name: "eq",
                arguments: vec!["hello", "hello"],
                expected_results: "",
            },
            FunctionExample {
                name: "neq",
                arguments: vec!["hello", "world"],
                expected_results: "hello",
            },
        ]
    }
}

struct Lower {}
impl Operator for Lower {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        args.first()
            .and_then(|f| f.as_string())
            .map(|f| f.to_lowercase())
            .into()
    }
    fn max_args(&self) -> Option<usize> {
        Some(1)
    }
    fn min_args(&self) -> usize {
        1
    }
    fn name(&self) -> &str {
        "LOWER"
    }

    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<FunctionExample<'a>> {
        vec![
            FunctionExample {
                name: "str",
                arguments: vec!["HeLLo"],
                expected_results: "hello",
            },
            FunctionExample {
                name: "number",
                arguments: vec!["123"],
                expected_results: "",
            },
        ]
    }
}
struct Upper {}
impl Operator for Upper {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        args.first()
            .and_then(|f| f.as_string())
            .map(|f| f.to_uppercase())
            .into()
    }
    fn max_args(&self) -> Option<usize> {
        Some(1)
    }
    fn min_args(&self) -> usize {
        1
    }
    fn name(&self) -> &str {
        "UPPER"
    }

    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<FunctionExample<'a>> {
        vec![
            FunctionExample {
                name: "str",
                arguments: vec!["HeLLo"],
                expected_results: "HELLO",
            },
            FunctionExample {
                name: "number",
                arguments: vec!["123"],
                expected_results: "",
            },
        ]
    }
}

struct Least {}
impl Operator for Least {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let mut least = None;
        for a in args.iter() {
            if !a.is_empty() {
                match &least {
                    None => least = Some(a.deref().clone()),
                    Some(val) => {
                        if a.deref() < val {
                            least = Some(a.deref().clone());
                        }
                    }
                }
            }
        }
        match least {
            None => Value::Empty.into(),
            Some(least) => least.into(),
        }
    }
    fn max_args(&self) -> Option<usize> {
        None
    }
    fn min_args(&self) -> usize {
        0
    }
    fn name(&self) -> &str {
        "LEAST"
    }

    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<FunctionExample<'a>> {
        vec![
            FunctionExample {
                name: "least",
                arguments: vec!["10", "400040", "1044", "-4", "-1"],
                expected_results: "-4",
            },
            FunctionExample {
                name: "empty",
                arguments: vec![],
                expected_results: "",
            },
        ]
    }
}

struct Left {}
impl Operator for Left {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let text = args.first();
        let Some(text) = text.as_string() else {
            return Value::Empty.into();
        };
        let length = args.get(1);
        let Some(length) = length.as_usize() else {
            return Value::Empty.into();
        };
        if text.len() < length {
            Value::Str(text.to_string()).into()
        } else {
            Value::Str(text[0..length].to_string()).into()
        }
    }
    fn max_args(&self) -> Option<usize> {
        Some(2)
    }
    fn min_args(&self) -> usize {
        2
    }
    fn name(&self) -> &str {
        "LEFT"
    }

    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<FunctionExample<'a>> {
        vec![
            FunctionExample {
                name: "simple",
                arguments: vec!["test", "2"],
                expected_results: "te",
            },
            FunctionExample {
                name: "exact",
                arguments: vec!["test", "4"],
                expected_results: "test",
            },
            FunctionExample {
                name: "more",
                arguments: vec!["test", "12"],
                expected_results: "test",
            },
            FunctionExample {
                name: "nan",
                arguments: vec!["test", "five"],
                expected_results: "",
            },
            FunctionExample {
                name: "not_a_text",
                arguments: vec!["10", "10"],
                expected_results: "",
            },
        ]
    }
}

struct Right {}
impl Operator for Right {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let text = args.first();
        let Some(text) = text.as_string() else {
            return Value::Empty.into();
        };
        let length = args.get(1);
        let Some(length) = length.as_usize() else {
            return Value::Empty.into();
        };
        if text.len() < length {
            Value::Str(text.to_string()).into()
        } else {
            let start = text.len() - length;
            Value::Str(text[start..].to_string()).into()
        }
    }
    fn max_args(&self) -> Option<usize> {
        Some(2)
    }
    fn min_args(&self) -> usize {
        2
    }
    fn name(&self) -> &str {
        "RIGHT"
    }

    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<FunctionExample<'a>> {
        vec![
            FunctionExample {
                name: "simple",
                arguments: vec!["test", "3"],
                expected_results: "est",
            },
            FunctionExample {
                name: "exact",
                arguments: vec!["test", "4"],
                expected_results: "test",
            },
            FunctionExample {
                name: "more",
                arguments: vec!["test", "12"],
                expected_results: "test",
            },
            FunctionExample {
                name: "nan",
                arguments: vec!["test", "five"],
                expected_results: "",
            },
            FunctionExample {
                name: "not_a_text",
                arguments: vec!["10", "10"],
                expected_results: "",
            },
        ]
    }
}

struct Lpad {}
impl Operator for Lpad {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let text = args.first();
        let Some(text) = text.as_string() else {
            return Value::Empty.into();
        };
        let length = args.get(1);
        let Some(length) = length.as_usize() else {
            return Value::Empty.into();
        };
        let pad = args.get(2);

        let Some(pad) = pad.as_string() else {
            return Value::Empty.into();
        };

        if text.len() > length {
            Value::Str(text[0..length].to_string()).into()
        } else if pad.is_empty() {
            Value::Str(text.to_string()).into()
        } else {
            let mut str = String::new();
            let mut chars = pad.chars().cycle();
            for _ in 0..length - text.len() {
                let chr = chars.next().unwrap();
                str.push(chr);
            }
            str.push_str(text);
            Value::Str(str).into()
        }
    }

    fn max_args(&self) -> Option<usize> {
        Some(3)
    }
    fn min_args(&self) -> usize {
        3
    }
    fn name(&self) -> &str {
        "LPAD"
    }

    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<FunctionExample<'a>> {
        vec![
            FunctionExample {
                name: "simple",
                arguments: vec!["text", "10", "pad"],
                expected_results: "padpadtext",
            },
            FunctionExample {
                name: "more",
                arguments: vec!["text", "12", "pad"],
                expected_results: "padpadpatext",
            },
            FunctionExample {
                name: "less",
                arguments: vec!["text", "3", "pad"],
                expected_results: "tex",
            },
            FunctionExample {
                name: "exact",
                arguments: vec!["text", "4", "pad"],
                expected_results: "text",
            },
            FunctionExample {
                name: "negative",
                arguments: vec!["text", "-122", "pad"],
                expected_results: "",
            },
            FunctionExample {
                name: "non_text",
                arguments: vec!["12", "10", "pad"],
                expected_results: "",
            },
            FunctionExample {
                name: "non_number",
                arguments: vec!["text", "me", "pad"],
                expected_results: "",
            },
            FunctionExample {
                name: "not_pad",
                arguments: vec!["text", "10", "2"],
                expected_results: "",
            },
            FunctionExample {
                name: "empty_pad",
                arguments: vec!["text", "10", "2"],
                expected_results: "",
            },
        ]
    }
}

struct Rpad {}
impl Operator for Rpad {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let text = args.first();
        let Some(text) = text.as_string() else {
            return Value::Empty.into();
        };
        let length = args.get(1);
        let Some(length) = length.as_usize() else {
            return Value::Empty.into();
        };
        let pad = args.get(2);

        let Some(pad) = pad.as_string() else {
            return Value::Empty.into();
        };

        if text.len() > length {
            Value::Str(text[0..length].to_string()).into()
        } else if pad.is_empty() {
            Value::Str(text.to_string()).into()
        } else {
            let mut str = text.to_string();
            let mut chars = pad.chars().cycle();
            for _ in 0..length - text.len() {
                let chr = chars.next().unwrap();
                str.push(chr);
            }
            Value::Str(str).into()
        }
    }

    fn max_args(&self) -> Option<usize> {
        Some(3)
    }
    fn min_args(&self) -> usize {
        3
    }
    fn name(&self) -> &str {
        "RPAD"
    }

    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<FunctionExample<'a>> {
        vec![
            FunctionExample {
                name: "simple",
                arguments: vec!["text", "10", "pad"],
                expected_results: "textpadpad",
            },
            FunctionExample {
                name: "more",
                arguments: vec!["text", "12", "pad"],
                expected_results: "textpadpadpa",
            },
            FunctionExample {
                name: "less",
                arguments: vec!["text", "3", "pad"],
                expected_results: "tex",
            },
            FunctionExample {
                name: "exact",
                arguments: vec!["text", "4", "pad"],
                expected_results: "text",
            },
            FunctionExample {
                name: "negative",
                arguments: vec!["text", "-122", "pad"],
                expected_results: "",
            },
            FunctionExample {
                name: "non_text",
                arguments: vec!["12", "10", "pad"],
                expected_results: "",
            },
            FunctionExample {
                name: "non_number",
                arguments: vec!["text", "me", "pad"],
                expected_results: "",
            },
            FunctionExample {
                name: "not_pad",
                arguments: vec!["text", "10", "2"],
                expected_results: "",
            },
            FunctionExample {
                name: "empty_pad",
                arguments: vec!["text", "10", "2"],
                expected_results: "",
            },
        ]
    }
}

struct Ltrim {}
impl Operator for Ltrim {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        args.first()
            .and_then(|f| f.as_string())
            .map(|f| f.trim_start())
            .into()
    }

    fn max_args(&self) -> Option<usize> {
        Some(1)
    }
    fn min_args(&self) -> usize {
        1
    }
    fn name(&self) -> &str {
        "LTRIM"
    }

    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<FunctionExample<'a>> {
        vec![
            FunctionExample {
                name: "simple",
                arguments: vec!["  hello"],
                expected_results: "hello",
            },
            FunctionExample {
                name: "not_text",
                arguments: vec!["12"],
                expected_results: "",
            },
        ]
    }
}
struct Rtrim {}
impl Operator for Rtrim {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        args.first()
            .and_then(|f| f.as_string())
            .map(|f| f.trim_end())
            .into()
    }

    fn max_args(&self) -> Option<usize> {
        Some(1)
    }
    fn min_args(&self) -> usize {
        1
    }
    fn name(&self) -> &str {
        "RTRIM"
    }

    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<FunctionExample<'a>> {
        vec![
            FunctionExample {
                name: "simple",
                arguments: vec!["hello\t"],
                expected_results: "hello",
            },
            FunctionExample {
                name: "not_text",
                arguments: vec!["12"],
                expected_results: "",
            },
        ]
    }
}

struct Position {}
impl Operator for Position {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let sub = args.first();
        let Some(sub) = sub.as_string() else {
            return Value::Empty.into();
        };
        let str = args.get(1);
        let Some(str) = str.as_string() else {
            return Value::Empty.into();
        };
        let start = match args.get(2) {
            None => 0,
            Some(val) => {
                let Some(mut start) = val.as_usize() else {
                    return Value::Empty.into();
                };
                if start == 0 {
                    start = 1;
                }
                if start > str.len() {
                    return 0.into();
                }
                start - 1
            }
        };
        let position = str[start..].find(sub).map(|f| f + 1).unwrap_or_default();
        (position + start).into()
    }

    fn max_args(&self) -> Option<usize> {
        Some(3)
    }
    fn min_args(&self) -> usize {
        2
    }
    fn name(&self) -> &str {
        "POSITION"
    }

    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<FunctionExample<'a>> {
        vec![
            FunctionExample {
                name: "simple",
                arguments: vec!["bar", "foobarbar"],
                expected_results: "4",
            },
            FunctionExample {
                name: "nop",
                arguments: vec!["xbar", "foobarbar"],
                expected_results: "0",
            },
            FunctionExample {
                name: "with_start",
                arguments: vec!["bar", "foobarbar", "5"],
                expected_results: "7",
            },
            FunctionExample {
                name: "not_a_sub",
                arguments: vec!["5", "foobarbar", "5"],
                expected_results: "",
            },
            FunctionExample {
                name: "with_str_as_num",
                arguments: vec!["bar", "20", "5"],
                expected_results: "",
            },
            FunctionExample {
                name: "with_start_as_str",
                arguments: vec!["bar", "foobarbar", "a"],
                expected_results: "",
            },
            FunctionExample {
                name: "with_start_larger",
                arguments: vec!["bar", "foobarbar", "25"],
                expected_results: "0",
            },
        ]
    }
}

struct Repeat {}
impl Operator for Repeat {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let str = args.first();
        let Some(str) = str.as_string() else {
            return Value::Empty.into();
        };
        let count = args.get(1);
        let Some(count) = count.as_usize() else {
            return Value::Empty.into();
        };
        Value::Str(str.repeat(count)).into()
    }

    fn max_args(&self) -> Option<usize> {
        Some(2)
    }
    fn min_args(&self) -> usize {
        2
    }
    fn name(&self) -> &str {
        "REPEAT"
    }

    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<FunctionExample<'a>> {
        vec![
            FunctionExample {
                name: "simple",
                arguments: vec!["bar", "3"],
                expected_results: "barbarbar",
            },
            FunctionExample {
                name: "not_a_string",
                arguments: vec!["4", "3"],
                expected_results: "",
            },
            FunctionExample {
                name: "not_a_number",
                arguments: vec!["bar", "test"],
                expected_results: "",
            },
        ]
    }
}

struct Replace {}
impl Operator for Replace {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let str = args.first();
        let Some(str) = str.as_string() else {
            return Value::Empty.into();
        };
        let what = args.get(1);
        let Some(what) = what.as_string() else {
            return Value::Empty.into();
        };
        let into = args.get(2);
        let Some(into) = into.as_string() else {
            return Value::Empty.into();
        };
        Value::Str(str.replace(what, into)).into()
    }

    fn max_args(&self) -> Option<usize> {
        Some(3)
    }
    fn min_args(&self) -> usize {
        3
    }
    fn name(&self) -> &str {
        "REPLACE"
    }

    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<FunctionExample<'a>> {
        vec![
            FunctionExample {
                name: "simple",
                arguments: vec!["hello", "l", "L"],
                expected_results: "heLLo",
            },
            FunctionExample {
                name: "nope",
                arguments: vec!["test", "bar", "foo"],
                expected_results: "test",
            },
            FunctionExample {
                name: "not_a_string",
                arguments: vec!["1", "test", "one"],
                expected_results: "",
            },
            FunctionExample {
                name: "not_a_what",
                arguments: vec!["bar", "2", "one"],
                expected_results: "",
            },
            FunctionExample {
                name: "not_a_with",
                arguments: vec!["bar", "test", "3"],
                expected_results: "",
            },
        ]
    }
}

struct RegexReplace {}
impl Operator for RegexReplace {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let str = args.first();
        let Some(str) = str.as_string() else {
            return Value::Empty.into();
        };
        let pattern = args.get(1);
        let Some(pattern) = pattern.as_string() else {
            return Value::Empty.into();
        };
        let Ok(pattern) = Regex::new(pattern) else {
            return Value::Empty.into();
        };
        let repl = args.get(2);
        let Some(repl) = repl.as_string() else {
            return Value::Empty.into();
        };
        Value::Str(pattern.replace(str, repl).to_string()).into()
    }

    fn max_args(&self) -> Option<usize> {
        Some(3)
    }
    fn min_args(&self) -> usize {
        3
    }
    fn name(&self) -> &str {
        "REGEX_REPLACE"
    }

    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<FunctionExample<'a>> {
        vec![
            FunctionExample {
                name: "simple",
                arguments: vec!["a b c", "b", "B"],
                expected_results: "a B c",
            },
            FunctionExample {
                name: "few",
                arguments: vec!["test", "[a-z]", "T"],
                expected_results: "Test",
            },
            FunctionExample {
                name: "not_a_string",
                arguments: vec!["1", "b", "B"],
                expected_results: "",
            },
            FunctionExample {
                name: "not_a_string_pattern",
                arguments: vec!["abc", "1", "B"],
                expected_results: "",
            },
            FunctionExample {
                name: "invalid_pattern",
                arguments: vec!["abc", "[+", "B"],
                expected_results: "",
            },
            FunctionExample {
                name: "no_replacement",
                arguments: vec!["abc", "b", "4"],
                expected_results: "",
            },
        ]
    }
}

struct RegexLike {}
impl Operator for RegexLike {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let expr = args.first();
        let Some(expr) = expr.as_string() else {
            return Value::Empty.into();
        };
        let pattern = args.get(1);
        let Some(pattern) = pattern.as_string() else {
            return Value::Empty.into();
        };
        let flags = args.get(2);
        let pattern = if flags.is_some() {
            let Some(flags) = flags.as_string() else {
                return Value::Empty.into();
            };

            Regex::new(format!("(?{}:{})", flags, pattern).as_str())
        } else {
            Regex::new(pattern)
        };

        let Ok(pattern) = pattern else {
            return Value::Empty.into();
        };

        Value::Bool(pattern.find(expr).is_some()).into()
    }

    fn max_args(&self) -> Option<usize> {
        Some(3)
    }
    fn min_args(&self) -> usize {
        2
    }
    fn name(&self) -> &str {
        "REGEX_LIKE"
    }

    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<FunctionExample<'a>> {
        vec![
            FunctionExample {
                name: "simple_true",
                arguments: vec!["test", "es"],
                expected_results: "TRUE",
            },
            FunctionExample {
                name: "simple_false",
                arguments: vec!["test", "ES"],
                expected_results: "FALSE",
            },
            FunctionExample {
                name: "no_string_one",
                arguments: vec!["1", "op"],
                expected_results: "",
            },
            FunctionExample {
                name: "no_string_pattern",
                arguments: vec!["test", "FALSE"],
                expected_results: "",
            },
            FunctionExample {
                name: "invalid_pattern",
                arguments: vec!["test", "[+"],
                expected_results: "",
            },
            FunctionExample {
                name: "with_flags",
                arguments: vec!["test", "TEST", "i"],
                expected_results: "TRUE",
            },
            FunctionExample {
                name: "with_flags",
                arguments: vec!["test", "TEST", "i"],
                expected_results: "TRUE",
            },
            FunctionExample {
                name: "with_invalid_flags",
                arguments: vec!["test", "TEST", "q"],
                expected_results: "",
            },
            FunctionExample {
                name: "with_no_string_flags",
                arguments: vec!["test", "TEST", "TRUE"],
                expected_results: "",
            },
        ]
    }
}

struct RegexSubstring {}
impl Operator for RegexSubstring {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let expr = args.first();
        let Some(expr) = expr.as_string() else {
            return Value::Empty.into();
        };
        let pattern = args.get(1);
        let Some(pattern) = pattern.as_string() else {
            return Value::Empty.into();
        };
        let pos = args.get(2);
        let pos = match pos {
            None => Some(1),
            Some(pos) => pos.as_usize(),
        };
        let Some(pos) = pos else {
            return Value::Empty.into();
        };
        if pos == 0 {
            return Value::Empty.into();
        }
        let pos = pos - 1;
        let occurrence = args.get(3);
        let occurrence = match occurrence {
            None => Some(1),
            Some(occurrence) => occurrence.as_usize(),
        };
        let Some(occurrence) = occurrence else {
            return Value::Empty.into();
        };
        if occurrence == 0 {
            return Value::Empty.into();
        }
        let occurrence = occurrence - 1;

        let flags = args.get(4);
        let pattern = if flags.is_some() {
            let Some(flags) = flags.as_string() else {
                return Value::Empty.into();
            };

            Regex::new(format!("(?{}:{})", flags, pattern).as_str())
        } else {
            Regex::new(pattern)
        };
        if pos > expr.len() {
            return Value::Str(String::new()).into();
        }

        let Ok(pattern) = pattern else {
            return Value::Empty.into();
        };

        let matches = pattern
            .find_iter(&expr[pos..])
            .nth(occurrence)
            .map(|f| f.as_str())
            .unwrap_or("");

        Value::Str(matches.into()).into()
    }

    fn max_args(&self) -> Option<usize> {
        Some(5)
    }
    fn min_args(&self) -> usize {
        2
    }
    fn name(&self) -> &str {
        "REGEXP_SUBSTR"
    }

    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<FunctionExample<'a>> {
        vec![
            FunctionExample {
                name: "simple_true",
                arguments: vec!["abc def ghi", "[a-z]+"],
                expected_results: "abc",
            },
            FunctionExample {
                name: "can_not_find",
                arguments: vec!["test", "ES"],
                expected_results: "\"\"",
            },
            FunctionExample {
                name: "with_pos",
                arguments: vec!["abc def ghi", "[a-z]+", "2"],
                expected_results: "bc",
            },
            FunctionExample {
                name: "with_pos_and_oc",
                arguments: vec!["abc def ghi", "[a-z]+", "2", "2"],
                expected_results: "def",
            },
            FunctionExample {
                name: "with_pos_and_oc_to_large",
                arguments: vec!["abc def ghi", "[a-z]+", "2", "10"],
                expected_results: "\"\"",
            },
            FunctionExample {
                name: "with_flags",
                arguments: vec!["abc def ghi", "[A-Z]+", "1", "1", "i"],
                expected_results: "abc",
            },
            FunctionExample {
                name: "invalid_str",
                arguments: vec!["1", "[A-Z]+", "1", "1", "i"],
                expected_results: "",
            },
            FunctionExample {
                name: "no_regex",
                arguments: vec!["abc def ghi", "2", "1", "1", "i"],
                expected_results: "",
            },
            FunctionExample {
                name: "invalid_regex",
                arguments: vec!["abc def ghi", "[A-", "1", "1", "i"],
                expected_results: "",
            },
            FunctionExample {
                name: "invalid_pos",
                arguments: vec!["abc def ghi", "[A-Z]+", "a", "1", "i"],
                expected_results: "",
            },
            FunctionExample {
                name: "post_zero",
                arguments: vec!["abc def ghi", "[A-Z]+", "0", "1", "i"],
                expected_results: "",
            },
            FunctionExample {
                name: "invalid_oc",
                arguments: vec!["abc def ghi", "[A-Z]+", "1", "A", "i"],
                expected_results: "",
            },
            FunctionExample {
                name: "zero_oc",
                arguments: vec!["abc def ghi", "[A-Z]+", "1", "0", "i"],
                expected_results: "",
            },
            FunctionExample {
                name: "invalid_falgs",
                arguments: vec!["abc def ghi", "[A-Z]+", "1", "0", "2"],
                expected_results: "",
            },
            FunctionExample {
                name: "unknown_flags",
                arguments: vec!["abc def ghi", "[A-Z]+", "1", "0", "1"],
                expected_results: "",
            },
        ]
    }
}

struct Reverse {}
impl Operator for Reverse {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let str = args.first();
        let Some(str) = str.as_string() else {
            return Value::Empty.into();
        };
        Value::Str(str.chars().rev().collect()).into()
    }

    fn max_args(&self) -> Option<usize> {
        Some(1)
    }
    fn min_args(&self) -> usize {
        1
    }
    fn name(&self) -> &str {
        "REVERSE"
    }

    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<FunctionExample<'a>> {
        vec![
            FunctionExample {
                name: "simple",
                arguments: vec!["simple"],
                expected_results: "elpmis",
            },
            FunctionExample {
                name: "nopre",
                arguments: vec!["323"],
                expected_results: "",
            },
        ]
    }
}
struct Round {}
impl Operator for Round {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let num = args.first();
        let Some(num) = num.as_num() else {
            return Value::Empty.into();
        };
        let digit = args.get(1);
        let digit = if digit.is_some() {
            digit.as_i64()
        } else {
            Some(0)
        };
        let Some(digit) = digit else {
            return Value::Empty.into();
        };

        Value::Number(num.with_scale_round(digit, bigdecimal::RoundingMode::HalfDown)).into()
    }

    fn max_args(&self) -> Option<usize> {
        Some(2)
    }
    fn min_args(&self) -> usize {
        1
    }
    fn name(&self) -> &str {
        "ROUND"
    }

    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<FunctionExample<'a>> {
        vec![
            FunctionExample {
                name: "simple",
                arguments: vec!["-1.23"],
                expected_results: "-1",
            },
            FunctionExample {
                name: "larger",
                arguments: vec!["43.123"],
                expected_results: "43",
            },
            FunctionExample {
                name: "go_up",
                arguments: vec!["43.6123"],
                expected_results: "44",
            },
            FunctionExample {
                name: "with_arg",
                arguments: vec![".1234567890123456789012345678901234567890", "35"],
                expected_results: "0.12345678901234567890123456789012346",
            },
            FunctionExample {
                name: "with_negative_arg",
                arguments: vec!["23.298", "-1"],
                expected_results: "20",
            },
            FunctionExample {
                name: "nan1",
                arguments: vec!["test"],
                expected_results: "",
            },
            FunctionExample {
                name: "nan2",
                arguments: vec!["1", "test"],
                expected_results: "",
            },
        ]
    }
}
struct Sqrt {}
impl Operator for Sqrt {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let num = args.first();
        let Some(num) = num.as_num() else {
            return Value::Empty.into();
        };
        match num.sqrt() {
            Some(num) => Value::Number(num),
            None => Value::Empty,
        }
        .into()
    }

    fn max_args(&self) -> Option<usize> {
        Some(1)
    }
    fn min_args(&self) -> usize {
        1
    }
    fn name(&self) -> &str {
        "SQRT"
    }

    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<FunctionExample<'a>> {
        vec![
            FunctionExample {
                name: "simple",
                arguments: vec!["16"],
                expected_results: "4",
            },
            FunctionExample {
                name: "larger",
                arguments: vec!["121"],
                expected_results: "11",
            },
            FunctionExample {
                name: "neg",
                arguments: vec!["-4"],
                expected_results: "",
            },
            FunctionExample {
                name: "nan",
                arguments: vec!["test"],
                expected_results: "",
            },
        ]
    }
}

struct Pi {}
impl Operator for Pi {
    fn get<'a>(&'a self, _: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let pi = BigDecimal::from_str("3.1415926535897932384626433832795").unwrap();
        Value::Number(pi).into()
    }

    fn max_args(&self) -> Option<usize> {
        Some(0)
    }
    fn min_args(&self) -> usize {
        0
    }
    fn name(&self) -> &str {
        "PI"
    }
}

struct Exp {}
impl Operator for Exp {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let num = args.first();
        let Some(num) = num.as_num() else {
            return Value::Empty.into();
        };

        Value::Number(num.exp()).into()
    }

    fn max_args(&self) -> Option<usize> {
        Some(1)
    }
    fn min_args(&self) -> usize {
        1
    }
    fn name(&self) -> &str {
        "EXP"
    }
}

struct Ln {}
impl Operator for Ln {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let num = args.first();
        let Some(num) = num.as_f64() else {
            return Value::Empty.into();
        };
        if num <= 0.0 {
            return Value::Empty.into();
        }

        num.ln().into()
    }

    fn max_args(&self) -> Option<usize> {
        Some(1)
    }
    fn min_args(&self) -> usize {
        1
    }
    fn name(&self) -> &str {
        "LN"
    }
}
struct Log {}
impl Operator for Log {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let (base, num) = if args.len() == 2 {
            let base = args.first();
            let Some(base) = base.as_f64() else {
                return Value::Empty.into();
            };
            let num = args.get(1);
            let Some(num) = num.as_f64() else {
                return Value::Empty.into();
            };
            if base <= 0.0 {
                return Value::Empty.into();
            }
            (base, num)
        } else {
            let num = args.first();
            let Some(num) = num.as_f64() else {
                return Value::Empty.into();
            };
            (10.0, num)
        };
        if num <= 0.0 {
            return Value::Empty.into();
        }

        num.log(base).into()
    }

    fn max_args(&self) -> Option<usize> {
        Some(2)
    }
    fn min_args(&self) -> usize {
        1
    }
    fn name(&self) -> &str {
        "LOG"
    }
    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<FunctionExample<'a>> {
        vec![
            FunctionExample {
                name: "simple",
                arguments: vec!["100"],
                expected_results: "2",
            },
            FunctionExample {
                name: "zero",
                arguments: vec!["0"],
                expected_results: "",
            },
            FunctionExample {
                name: "not_a_num",
                arguments: vec!["test"],
                expected_results: "",
            },
            FunctionExample {
                name: "two_args",
                arguments: vec!["2", "8"],
                expected_results: "3",
            },
            FunctionExample {
                name: "invalid_base",
                arguments: vec!["0", "8"],
                expected_results: "",
            },
            FunctionExample {
                name: "no_base",
                arguments: vec!["", "8"],
                expected_results: "",
            },
        ]
    }
}

struct Log2 {}
impl Operator for Log2 {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let num = args.first();
        let Some(num) = num.as_f64() else {
            return Value::Empty.into();
        };
        if num <= 0.0 {
            return Value::Empty.into();
        }

        num.log2().into()
    }

    fn max_args(&self) -> Option<usize> {
        Some(1)
    }
    fn min_args(&self) -> usize {
        1
    }
    fn name(&self) -> &str {
        "LOG2"
    }
    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<FunctionExample<'a>> {
        vec![
            FunctionExample {
                name: "simple",
                arguments: vec!["16"],
                expected_results: "4",
            },
            FunctionExample {
                name: "zero",
                arguments: vec!["0"],
                expected_results: "",
            },
            FunctionExample {
                name: "not_a_num",
                arguments: vec!["test"],
                expected_results: "",
            },
        ]
    }
}
struct Log10 {}
impl Operator for Log10 {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let num = args.first();
        let Some(num) = num.as_f64() else {
            return Value::Empty.into();
        };
        if num <= 0.0 {
            return Value::Empty.into();
        }

        num.log10().into()
    }

    fn max_args(&self) -> Option<usize> {
        Some(1)
    }
    fn min_args(&self) -> usize {
        1
    }
    fn name(&self) -> &str {
        "LOG10"
    }
    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<FunctionExample<'a>> {
        vec![
            FunctionExample {
                name: "simple",
                arguments: vec!["100"],
                expected_results: "2",
            },
            FunctionExample {
                name: "zero",
                arguments: vec!["0"],
                expected_results: "",
            },
            FunctionExample {
                name: "not_a_num",
                arguments: vec!["test"],
                expected_results: "",
            },
        ]
    }
}

struct Power {}
impl Operator for Power {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let base = args.first();
        let Some(base) = base.as_f64() else {
            return Value::Empty.into();
        };
        let num = args.get(1);
        let Some(num) = num.as_f64() else {
            return Value::Empty.into();
        };
        if base <= 0.0 {
            return Value::Empty.into();
        }

        base.powf(num).into()
    }

    fn max_args(&self) -> Option<usize> {
        Some(2)
    }
    fn min_args(&self) -> usize {
        2
    }
    fn name(&self) -> &str {
        "POWER"
    }
    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<FunctionExample<'a>> {
        vec![
            FunctionExample {
                name: "simple",
                arguments: vec!["2", "3"],
                expected_results: "8",
            },
            FunctionExample {
                name: "negative",
                arguments: vec!["2", "-2"],
                expected_results: "0.25",
            },
            FunctionExample {
                name: "not_a_num_base",
                arguments: vec!["a", "2"],
                expected_results: "",
            },
            FunctionExample {
                name: "not_a_num_exp",
                arguments: vec!["2", "a"],
                expected_results: "",
            },
        ]
    }
}

struct Random {}
impl Operator for Random {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let rnd: f64 = rand::random();
        if args.is_empty() {
            rnd.into()
        } else {
            let Some(max) = args.first().as_usize() else {
                return Value::Empty.into();
            };
            if max == 0 {
                Value::Number(BigDecimal::zero()).into()
            } else {
                (rnd * max as f64).floor().into()
            }
        }
    }

    fn max_args(&self) -> Option<usize> {
        Some(1)
    }
    fn min_args(&self) -> usize {
        0
    }
    fn name(&self) -> &str {
        "RANDOM"
    }
}

#[cfg(test)]
mod tests_functions {
    use std::fs::{self, OpenOptions};

    use bigdecimal::ToPrimitive;
    use chrono::{TimeDelta, Utc};
    use itertools::Itertools;

    use crate::{args::Args, engine::Engine, error::CvsSqlError, results::Column, value::Value};
    use std::io::Write;

    use super::{
        Abs, Ascii, Chr, Coalece, Concat, ConcatWs, CurrentDate, Exp, Format, Greatest, If, Least,
        Left, Length, Ln, Log, Log2, Log10, Lower, Lpad, Ltrim, Now, NullIf, Operator, Pi,
        Position, Power, Random, RegexLike, RegexReplace, RegexSubstring, Repeat, Replace, Reverse,
        Right, Round, Rpad, Rtrim, Sqrt, ToTimestamp, Upper, User,
    };

    fn test_func(operator: &impl Operator) -> Result<(), CvsSqlError> {
        let dir = format!("./target/function_tests/{}", operator.name().to_lowercase());
        println!("testing: {}", operator.name());
        fs::remove_dir_all(&dir).ok();
        for example in operator.examples() {
            test_with_details(operator, &example.name, &example.arguments, |r| {
                let expected_results = if example.expected_results == "\"\"" {
                    Value::Str(String::new())
                } else {
                    example.expected_results.into()
                };
                if r == Some(&expected_results) {
                    true
                } else {
                    println!(
                        "Function {} example {}, results: {:?}, expecting: [{}]",
                        operator.name(),
                        &example.name,
                        r,
                        example.expected_results
                    );
                    false
                }
            })?;
        }
        fs::remove_dir_all(&dir).ok();

        Ok(())
    }

    fn test_with_details<F>(
        operator: &impl Operator,
        name: &str,
        arguments: &[&str],
        verify_results: F,
    ) -> Result<(), CvsSqlError>
    where
        F: FnOnce(Option<&Value>) -> bool,
    {
        println!("testing: {} with {}", operator.name(), name);
        let dir = format!("./target/function_tests/{}", operator.name().to_lowercase());
        fs::create_dir_all(&dir)?;
        let file = format!("{}/{}.csv", dir, name);
        let mut writer = OpenOptions::new().write(true).create(true).open(&file)?;
        let header = ('a'..'z')
            .take(arguments.len())
            .map(|c| format!("{}", c))
            .join(",");
        writeln!(writer, "{},name", header)?;
        let line = arguments.join(",");
        writeln!(writer, "{},{}", line, name)?;

        let table_name = format!(
            "target.function_tests.{}.{}",
            operator.name().to_lowercase(),
            name
        );
        let sql = format!(
            "SELECT {}({}) FROM {}\n",
            operator.name(),
            header,
            table_name
        );

        let args = Args::default();
        let engine = Engine::try_from(&args)?;

        let results = engine.execute_commands(&sql)?;

        fs::remove_file(file)?;

        let col = Column::from_index(0);
        let result = results
            .first()
            .and_then(|d| d.data.iter().next())
            .map(|d| d.get(&col));

        assert_eq!(true, verify_results(result));

        Ok(())
    }

    #[test]
    fn test_abs() -> Result<(), CvsSqlError> {
        test_func(&Abs {})
    }

    #[test]
    fn test_ascii() -> Result<(), CvsSqlError> {
        test_func(&Ascii {})
    }

    #[test]
    fn test_chr() -> Result<(), CvsSqlError> {
        test_func(&Chr {})
    }

    #[test]
    fn test_length() -> Result<(), CvsSqlError> {
        test_func(&Length {})
    }

    #[test]
    fn test_coalece() -> Result<(), CvsSqlError> {
        test_func(&Coalece {})
    }

    #[test]
    fn test_concat() -> Result<(), CvsSqlError> {
        test_func(&Concat {})
    }

    #[test]
    fn test_concat_ws() -> Result<(), CvsSqlError> {
        test_func(&ConcatWs {})
    }

    #[test]
    fn test_current_date() -> Result<(), CvsSqlError> {
        test_with_details(&CurrentDate {}, "current_date", &vec![], |r| match r {
            Some(Value::Date(dt)) => {
                let now = Utc::now().naive_utc().date();
                let to = now.succ_opt().unwrap();
                let from = now.pred_opt().unwrap();

                *dt >= from && *dt <= to
            }
            _ => false,
        })
    }

    #[test]
    fn test_now() -> Result<(), CvsSqlError> {
        test_with_details(&Now {}, "now", &vec![], |r| match r {
            Some(Value::Timestamp(dt)) => {
                let now = Utc::now().naive_utc();
                let to = now.checked_add_signed(TimeDelta::seconds(10)).unwrap();
                let from = now.checked_add_signed(TimeDelta::seconds(-10)).unwrap();

                *dt >= from && *dt <= to
            }
            _ => false,
        })
    }

    #[test]
    fn test_current_user() -> Result<(), CvsSqlError> {
        test_with_details(&User {}, "user", &vec![], |r| match r {
            Some(Value::Str(user)) => *user == whoami::username(),
            _ => false,
        })
    }

    #[test]
    fn test_format() -> Result<(), CvsSqlError> {
        test_func(&Format {})
    }

    #[test]
    fn test_to_timestamp() -> Result<(), CvsSqlError> {
        test_func(&ToTimestamp {})
    }

    #[test]
    fn test_greatest() -> Result<(), CvsSqlError> {
        test_func(&Greatest {})
    }

    #[test]
    fn test_if() -> Result<(), CvsSqlError> {
        test_func(&If {})
    }

    #[test]
    fn test_null_if() -> Result<(), CvsSqlError> {
        test_func(&NullIf {})
    }

    #[test]
    fn test_lower() -> Result<(), CvsSqlError> {
        test_func(&Lower {})
    }

    #[test]
    fn test_upper() -> Result<(), CvsSqlError> {
        test_func(&Upper {})
    }

    #[test]
    fn test_least() -> Result<(), CvsSqlError> {
        test_func(&Least {})
    }

    #[test]
    fn test_left() -> Result<(), CvsSqlError> {
        test_func(&Left {})
    }

    #[test]
    fn test_right() -> Result<(), CvsSqlError> {
        test_func(&Right {})
    }

    #[test]
    fn test_lpad() -> Result<(), CvsSqlError> {
        test_func(&Lpad {})
    }

    #[test]
    fn test_rpad() -> Result<(), CvsSqlError> {
        test_func(&Rpad {})
    }

    #[test]
    fn test_ltrim() -> Result<(), CvsSqlError> {
        test_func(&Ltrim {})
    }

    #[test]
    fn test_rtrim() -> Result<(), CvsSqlError> {
        test_func(&Rtrim {})
    }

    #[test]
    fn test_pi() -> Result<(), CvsSqlError> {
        test_with_details(&Pi {}, "pi", &vec![], |r| match r {
            Some(Value::Number(num)) => {
                num.to_f64().unwrap() > 3.14 && num.to_f64().unwrap() < 3.15
            }
            _ => false,
        })
    }

    #[test]
    fn test_ln() -> Result<(), CvsSqlError> {
        test_with_details(&Ln {}, "ln", &vec!["10"], |r| match r {
            Some(Value::Number(num)) => {
                num.to_f64().unwrap() > 2.30 && num.to_f64().unwrap() < 2.31
            }
            _ => false,
        })?;
        test_with_details(&Ln {}, "neg_ln", &vec!["-10"], |r| r == Some(&Value::Empty))?;
        test_with_details(&Ln {}, "nan", &vec![""], |r| r == Some(&Value::Empty))
    }

    #[test]
    fn test_exp() -> Result<(), CvsSqlError> {
        test_with_details(&Exp {}, "exp", &vec!["10"], |r| match r {
            Some(Value::Number(num)) => {
                num.to_f64().unwrap() > 22026.46 && num.to_f64().unwrap() < 22026.47
            }
            _ => false,
        })?;
        test_with_details(&Exp {}, "neg_exp", &vec!["-10"], |r| match r {
            Some(Value::Number(num)) => {
                num.to_f64().unwrap() > 4.53e-5 && num.to_f64().unwrap() < 4.54e-5
            }
            _ => false,
        })?;
        test_with_details(&Exp {}, "nan", &vec![""], |r| r == Some(&Value::Empty))
    }

    #[test]
    fn test_log() -> Result<(), CvsSqlError> {
        test_func(&Log {})
    }

    #[test]
    fn test_log2() -> Result<(), CvsSqlError> {
        test_func(&Log2 {})
    }

    #[test]
    fn test_log10() -> Result<(), CvsSqlError> {
        test_func(&Log10 {})
    }

    #[test]
    fn test_power() -> Result<(), CvsSqlError> {
        test_func(&Power {})
    }

    #[test]
    fn test_position() -> Result<(), CvsSqlError> {
        test_func(&Position {})
    }

    #[test]
    fn test_repeat() -> Result<(), CvsSqlError> {
        test_func(&Repeat {})
    }

    #[test]
    fn test_replace() -> Result<(), CvsSqlError> {
        test_func(&Replace {})
    }

    #[test]
    fn test_regex_repalce() -> Result<(), CvsSqlError> {
        test_func(&RegexReplace {})
    }

    #[test]
    fn test_regex_like() -> Result<(), CvsSqlError> {
        test_func(&RegexLike {})
    }

    #[test]
    fn test_regex_substring() -> Result<(), CvsSqlError> {
        test_func(&RegexSubstring {})
    }

    #[test]
    fn test_reverese() -> Result<(), CvsSqlError> {
        test_func(&Reverse {})
    }
    #[test]
    fn test_rand() -> Result<(), CvsSqlError> {
        test_with_details(&Random {}, "no_args", &vec![], |r| match r {
            Some(Value::Number(num)) => num.to_f64().unwrap() > 0.0 && num.to_f64().unwrap() < 1.0,
            _ => false,
        })?;
        test_with_details(&Random {}, "one_args", &vec!["20"], |r| match r {
            Some(Value::Number(num)) => num.to_usize().unwrap() < 20,
            _ => false,
        })?;
        test_with_details(&Random {}, "nan", &vec!["t"], |r| r == Some(&Value::Empty))?;
        test_with_details(&Random {}, "neg", &vec!["-10"], |r| {
            r == Some(&Value::Empty)
        })
    }

    #[test]
    fn test_round() -> Result<(), CvsSqlError> {
        test_func(&Round {})
    }

    #[test]
    fn test_sqrt() -> Result<(), CvsSqlError> {
        test_func(&Sqrt {})
    }
}
