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
        "CURRENT_DATE" => build_function(metadata, engine, args, Box::new(CurrentDate {})),
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
        "LEAST" => build_function(metadata, engine, args, Box::new(Least {})),
        "LEFT" => build_function(metadata, engine, args, Box::new(Left {})),
        "LPAD" => build_function(metadata, engine, args, Box::new(Lpad {})),
        "LTRIM" => build_function(metadata, engine, args, Box::new(Ltrim {})),
        "SUBSTRING" | "MID" => build_function(metadata, engine, args, Box::new(SubString {})),
        "PI" => build_function(metadata, engine, args, Box::new(Pi {})),
        "POSITION" | "LOCATE" => build_function(metadata, engine, args, Box::new(Position {})),
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

        let args = Args {
            command: None,
            home: None,
            first_line_as_name: true,
        };
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

struct SubString {}
impl Operator for SubString {
    fn get<'a>(&'a self, args: &[SmartReference<'a, Value>]) -> SmartReference<'a, Value> {
        let text = args.first();
        let Some(text) = text.as_string() else {
            return Value::Empty.into();
        };

        let start = args.get(1);
        let Some(start) = start.as_usize() else {
            return Value::Empty.into();
        };

        if start > text.len() {
            return Value::Str(String::new()).into();
        }
        let text = &text[start - 1..];
        let length = match args.get(2) {
            None => text.len(),
            Some(length) => {
                let Some(length) = length.as_usize() else {
                    return Value::Empty.into();
                };
                length
            }
        };
        if length > text.len() {
            Value::Str(text.into())
        } else {
            Value::Str(text[..length].to_string())
        }
        .into()
    }

    fn max_args(&self) -> Option<usize> {
        Some(3)
    }
    fn min_args(&self) -> usize {
        2
    }
    fn name(&self) -> &str {
        "SUBSTRING"
    }

    #[cfg(test)]
    fn examples<'a>(&'a self) -> Vec<FunctionExample<'a>> {
        vec![
            FunctionExample {
                name: "start_only",
                arguments: vec!["abcdef", "3"],
                expected_results: "cdef",
            },
            FunctionExample {
                name: "start_only_negative",
                arguments: vec!["abcdef", "-3"],
                expected_results: "",
            },
            FunctionExample {
                name: "start_only_zero",
                arguments: vec!["abcdef", "0"],
                expected_results: "abcdef",
            },
            FunctionExample {
                name: "start_only_one",
                arguments: vec!["abcdef", "1"],
                expected_results: "abcdef",
            },
            FunctionExample {
                name: "start_only_large",
                arguments: vec!["abcdef", "20"],
                expected_results: "",
            },
            FunctionExample {
                name: "start_only_not_a_number",
                arguments: vec!["abcdef", "test"],
                expected_results: "",
            },
            FunctionExample {
                name: "start_only_not_text",
                arguments: vec!["204234", "2"],
                expected_results: "",
            },
            FunctionExample {
                name: "start_and_length",
                arguments: vec!["abcdef", "3", "2"],
                expected_results: "cd",
            },
            FunctionExample {
                name: "start_and_length_too_big",
                arguments: vec!["abcdef", "3", "20"],
                expected_results: "cdef",
            },
            FunctionExample {
                name: "start_and_length_exact",
                arguments: vec!["abcdef", "3", "4"],
                expected_results: "cdef",
            },
            FunctionExample {
                name: "start_and_length_negative",
                arguments: vec!["abcdef", "3", "-4"],
                expected_results: "",
            },
            FunctionExample {
                name: "start_and_length_text",
                arguments: vec!["abcdef", "3", "test"],
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

#[cfg(test)]
mod tests_functions {
    use std::fs::{self, OpenOptions};

    use bigdecimal::ToPrimitive;
    use chrono::{TimeDelta, Utc};
    use itertools::Itertools;

    use crate::{args::Args, engine::Engine, error::CvsSqlError, results::Column, value::Value};
    use std::io::Write;

    use super::{
        Abs, Ascii, Chr, Coalece, Concat, ConcatWs, CurrentDate, Format, Greatest, If, Least, Left,
        Length, Lower, Lpad, Ltrim, Now, NullIf, Operator, Pi, Position, SubString, ToTimestamp,
        User,
    };

    fn test_func(operator: &impl Operator) -> Result<(), CvsSqlError> {
        let dir = format!("./target/function_tests/{}", operator.name().to_lowercase());
        println!("testing: {}", operator.name());
        fs::remove_dir_all(&dir).ok();
        for example in operator.examples() {
            test_with_details(operator, &example.name, &example.arguments, |r| {
                let expected_results = example.expected_results.into();
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

        let args = Args {
            command: None,
            home: None,
            first_line_as_name: true,
        };
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
    fn test_least() -> Result<(), CvsSqlError> {
        test_func(&Least {})
    }

    #[test]
    fn test_left() -> Result<(), CvsSqlError> {
        test_func(&Left {})
    }

    #[test]
    fn test_lpad() -> Result<(), CvsSqlError> {
        test_func(&Lpad {})
    }

    #[test]
    fn test_ltrim() -> Result<(), CvsSqlError> {
        test_func(&Ltrim {})
    }

    #[test]
    fn test_substr() -> Result<(), CvsSqlError> {
        test_func(&SubString {})
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
    fn test_position() -> Result<(), CvsSqlError> {
        test_func(&Position {})
    }
}
