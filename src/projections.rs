use bigdecimal::{BigDecimal, FromPrimitive, ToPrimitive};
use regex::Regex;
use sqlparser::ast::{
    BinaryOperator, CaseWhen, CeilFloorKind, DateTimeField, Expr, SelectItem, SetExpr,
    UnaryOperator, WildcardAdditionalOptions,
};

use crate::cast::create_cast;
use crate::engine::Engine;
use crate::error::CvsSqlError;
use crate::extract_time::create_extract;
use crate::extractor::Extractor;
use crate::group_by::{GroupRow, GroupedResultSet};
use crate::result_set_metadata::{Metadata, SimpleResultSetMetadata};
use crate::results_data::{DataRow, ResultsData};
use crate::util::SmartReference;
use crate::{
    results::{Column, Name, ResultSet},
    value::Value,
};
use itertools::Itertools;
use sqlparser::ast::Value as AstValue;
use std::collections::HashSet;
use std::ops::Deref;
use std::rc::Rc;

pub(crate) trait Projection {
    fn get<'a>(&'a self, row: &'a GroupRow) -> SmartReference<'a, Value>;
    fn name(&self) -> &str;
}
struct ColumnProjection {
    column: Column,
    column_name: String,
}
impl Projection for ColumnProjection {
    fn get<'a>(&'a self, row: &'a GroupRow) -> SmartReference<'a, Value> {
        row.data.get(&self.column).into()
    }
    fn name(&self) -> &str {
        &self.column_name
    }
}

pub fn make_projection(
    engine: &Engine,
    parent: GroupedResultSet,
    items: &[SelectItem],
) -> Result<ResultSet, CvsSqlError> {
    let mut projections = Vec::new();
    let mut metadata = SimpleResultSetMetadata::new(parent.metadata.result_name().cloned());
    for item in items {
        let mut items = item.convert(&parent.metadata, engine)?;
        for i in &items {
            metadata.add_column(i.name());
        }
        projections.append(&mut items);
    }
    let metadata = metadata.build();
    let mut data = Vec::new();
    for parent_row in parent.rows.iter() {
        let mut row = Vec::new();
        for item in &projections {
            let data = item.get(parent_row);
            row.push(data.clone());
        }
        let row = DataRow::new(row);
        data.push(row);
    }
    let data = ResultsData::new(data);
    let metadata = Rc::new(metadata);
    Ok(ResultSet { metadata, data })
}
trait Convert {
    fn convert(
        &self,
        metadata: &Metadata,
        engine: &Engine,
    ) -> Result<Vec<Box<dyn Projection>>, CvsSqlError>;
}
impl Convert for SelectItem {
    fn convert(
        &self,
        metadata: &Metadata,
        engine: &Engine,
    ) -> Result<Vec<Box<dyn Projection>>, CvsSqlError> {
        match self {
            SelectItem::Wildcard(options) => options.convert(metadata, engine),
            SelectItem::UnnamedExpr(exp) => exp.convert(metadata, engine),
            SelectItem::ExprWithAlias { expr, alias } => {
                let data = expr.convert_single(metadata, engine)?;
                let alias = alias.value.to_string();
                Ok(vec![Box::new(AliasProjection { data, alias })])
            }
            SelectItem::QualifiedWildcard(_, _) => {
                Err(CvsSqlError::Unsupported(format!("Select {}", self)))
            }
        }
    }
}
impl Convert for WildcardAdditionalOptions {
    fn convert(
        &self,
        metadata: &Metadata,
        _: &Engine,
    ) -> Result<Vec<Box<dyn Projection>>, CvsSqlError> {
        if self.opt_ilike.is_some() {
            return Err(CvsSqlError::Unsupported("Select * ILIKE".into()));
        }
        if self.opt_exclude.is_some() {
            return Err(CvsSqlError::Unsupported("Select * EXCLUDE".into()));
        }
        if self.opt_except.is_some() {
            return Err(CvsSqlError::Unsupported("Select * EXCEPT".into()));
        }
        if self.opt_replace.is_some() {
            return Err(CvsSqlError::Unsupported("Select * REPLACE".into()));
        }
        if self.opt_rename.is_some() {
            return Err(CvsSqlError::Unsupported("Select * RENAME".into()));
        }
        let mut projections: Vec<Box<dyn Projection>> = Vec::new();
        for column in metadata.columns() {
            let Some(column_name) = metadata.column_name(&column) else {
                return Err(CvsSqlError::Unsupported(
                    "Select * with unnamed column".into(),
                ));
            };
            let column_name = column_name.short_name().to_string();
            projections.push(Box::new(ColumnProjection {
                column,
                column_name,
            }));
        }

        Ok(projections)
    }
}
pub trait SingleConvert {
    fn convert_single(
        &self,
        metadata: &Metadata,
        engine: &Engine,
    ) -> Result<Box<dyn Projection>, CvsSqlError>;
}

trait BinaryFunction {
    fn calculate(
        &self,
        left: SmartReference<Value>,
        right: SmartReference<Value>,
    ) -> SmartReference<Value>;
    fn name(&self) -> &str;
}

struct Plus {}
impl BinaryFunction for Plus {
    fn calculate<'a>(
        &'a self,
        left: SmartReference<Value>,
        right: SmartReference<Value>,
    ) -> SmartReference<'a, Value> {
        (left.deref() + right.deref()).into()
    }
    fn name(&self) -> &str {
        "+"
    }
}
struct Times {}
impl BinaryFunction for Times {
    fn calculate<'a>(
        &'a self,
        left: SmartReference<Value>,
        right: SmartReference<Value>,
    ) -> SmartReference<'a, Value> {
        (left.deref() * right.deref()).into()
    }
    fn name(&self) -> &str {
        "*"
    }
}
struct Divide {}
impl BinaryFunction for Divide {
    fn calculate<'a>(
        &'a self,
        left: SmartReference<Value>,
        right: SmartReference<Value>,
    ) -> SmartReference<'a, Value> {
        (left.deref() / right.deref()).into()
    }
    fn name(&self) -> &str {
        "/"
    }
}
struct TakeAway {}
impl BinaryFunction for TakeAway {
    fn calculate<'a>(
        &'a self,
        left: SmartReference<Value>,
        right: SmartReference<Value>,
    ) -> SmartReference<'a, Value> {
        (left.deref() - right.deref()).into()
    }
    fn name(&self) -> &str {
        "-"
    }
}
struct Modulo {}
impl BinaryFunction for Modulo {
    fn calculate<'a>(
        &'a self,
        left: SmartReference<Value>,
        right: SmartReference<Value>,
    ) -> SmartReference<'a, Value> {
        (left.deref() % right.deref()).into()
    }
    fn name(&self) -> &str {
        "%"
    }
}

struct ConcatOperator {}
impl BinaryFunction for ConcatOperator {
    fn calculate<'a>(
        &'a self,
        left: SmartReference<Value>,
        right: SmartReference<Value>,
    ) -> SmartReference<'a, Value> {
        let mut str = left.to_string();
        str.push_str(right.to_string().as_str());
        let value = Value::from(str.as_str());
        value.into()
    }
    fn name(&self) -> &str {
        "||"
    }
}

struct LessThen {}
impl BinaryFunction for LessThen {
    fn calculate<'a>(
        &'a self,
        left: SmartReference<Value>,
        right: SmartReference<Value>,
    ) -> SmartReference<'a, Value> {
        let comp = left.deref() < right.deref();
        let val: Value = comp.into();
        val.into()
    }
    fn name(&self) -> &str {
        "<"
    }
}

struct GreaterThen {}
impl BinaryFunction for GreaterThen {
    fn calculate<'a>(
        &'a self,
        left: SmartReference<Value>,
        right: SmartReference<Value>,
    ) -> SmartReference<'a, Value> {
        let comp = left.deref() > right.deref();
        let val: Value = comp.into();
        val.into()
    }
    fn name(&self) -> &str {
        ">"
    }
}
struct Equals {}
impl BinaryFunction for Equals {
    fn calculate<'a>(
        &'a self,
        left: SmartReference<Value>,
        right: SmartReference<Value>,
    ) -> SmartReference<'a, Value> {
        let comp = left.deref() == right.deref();
        let val: Value = comp.into();
        val.into()
    }
    fn name(&self) -> &str {
        "="
    }
}

struct LessThenEq {}
impl BinaryFunction for LessThenEq {
    fn calculate<'a>(
        &'a self,
        left: SmartReference<Value>,
        right: SmartReference<Value>,
    ) -> SmartReference<'a, Value> {
        let comp = left.deref() <= right.deref();
        let val: Value = comp.into();
        val.into()
    }
    fn name(&self) -> &str {
        "<="
    }
}

struct GreaterThenEq {}
impl BinaryFunction for GreaterThenEq {
    fn calculate<'a>(
        &'a self,
        left: SmartReference<Value>,
        right: SmartReference<Value>,
    ) -> SmartReference<'a, Value> {
        let comp = left.deref() >= right.deref();
        let val: Value = comp.into();
        val.into()
    }
    fn name(&self) -> &str {
        ">="
    }
}

struct NotEquals {}
impl BinaryFunction for NotEquals {
    fn calculate<'a>(
        &'a self,
        left: SmartReference<Value>,
        right: SmartReference<Value>,
    ) -> SmartReference<'a, Value> {
        let comp = left.deref() != right.deref();
        let val: Value = comp.into();
        val.into()
    }
    fn name(&self) -> &str {
        "<>"
    }
}

struct AndBinaryFunction {}
impl BinaryFunction for AndBinaryFunction {
    fn calculate<'a>(
        &'a self,
        left: SmartReference<Value>,
        right: SmartReference<Value>,
    ) -> SmartReference<'a, Value> {
        let results = match (left.deref(), right.deref()) {
            (&Value::Bool(true), &Value::Bool(true)) => Value::Bool(true),
            (&Value::Bool(_), &Value::Bool(_)) => Value::Bool(false),
            _ => Value::Empty,
        };
        results.into()
    }
    fn name(&self) -> &str {
        "AND"
    }
}

struct OrBinaryFunction {}
impl BinaryFunction for OrBinaryFunction {
    fn calculate<'a>(
        &'a self,
        left: SmartReference<Value>,
        right: SmartReference<Value>,
    ) -> SmartReference<'a, Value> {
        let results = match (left.deref(), right.deref()) {
            (&Value::Bool(false), &Value::Bool(false)) => Value::Bool(false),
            (&Value::Bool(_), &Value::Bool(_)) => Value::Bool(true),
            _ => Value::Empty,
        };
        results.into()
    }
    fn name(&self) -> &str {
        "OR"
    }
}

struct XorBinaryFunction {}
impl BinaryFunction for XorBinaryFunction {
    fn calculate<'a>(
        &'a self,
        left: SmartReference<Value>,
        right: SmartReference<Value>,
    ) -> SmartReference<'a, Value> {
        let results = match (left.deref(), right.deref()) {
            (&Value::Bool(false), &Value::Bool(true)) => Value::Bool(true),
            (&Value::Bool(true), &Value::Bool(false)) => Value::Bool(true),
            (&Value::Bool(_), &Value::Bool(_)) => Value::Bool(false),
            _ => Value::Empty,
        };
        results.into()
    }
    fn name(&self) -> &str {
        "XOR"
    }
}

struct AliasProjection {
    data: Box<dyn Projection>,
    alias: String,
}
impl Projection for AliasProjection {
    fn get<'a>(&'a self, row: &'a GroupRow) -> SmartReference<'a, Value> {
        self.data.get(row)
    }
    fn name(&self) -> &str {
        &self.alias
    }
}
struct BinaryProjection {
    left: Box<dyn Projection>,
    right: Box<dyn Projection>,
    operator: Box<dyn BinaryFunction>,
    name: String,
}
impl Projection for BinaryProjection {
    fn name(&self) -> &str {
        &self.name
    }
    fn get<'a>(&'a self, row: &'a GroupRow) -> SmartReference<'a, Value> {
        let left = self.left.get(row);
        let right = self.right.get(row);
        self.operator.calculate(left, right)
    }
}
impl BinaryProjection {
    fn new(
        left: Box<dyn Projection>,
        right: Box<dyn Projection>,
        operator: Box<dyn BinaryFunction>,
    ) -> Self {
        let name = format!("{} {} {}", left.name(), operator.name(), right.name());
        Self {
            left,
            right,
            operator,
            name,
        }
    }
}

impl<T: SingleConvert> Convert for T {
    fn convert(
        &self,
        metadata: &Metadata,
        engine: &Engine,
    ) -> Result<Vec<Box<dyn Projection>>, CvsSqlError> {
        let result = self.convert_single(metadata, engine)?;
        Ok(vec![result])
    }
}
struct ValueProjection {
    value: Value,
    name: String,
}
impl Projection for ValueProjection {
    fn get<'a>(&'a self, _: &GroupRow) -> SmartReference<'a, Value> {
        SmartReference::Borrowed(&self.value)
    }
    fn name(&self) -> &str {
        &self.name
    }
}

trait UnaryFunction {
    fn calculate(&self, value: SmartReference<Value>) -> SmartReference<Value>;
    fn name(&self) -> &str;
    fn function_type(&self) -> UnaryFunctionType;
}

enum UnaryFunctionType {
    Prefix,
    Postfix,
    Function,
}
struct UnartyProjection {
    value: Box<dyn Projection>,
    operator: Box<dyn UnaryFunction>,
    name: String,
}

impl Projection for UnartyProjection {
    fn get<'a>(&'a self, row: &'a GroupRow) -> SmartReference<'a, Value> {
        let value = self.value.get(row);
        self.operator.calculate(value)
    }
    fn name(&self) -> &str {
        &self.name
    }
}
impl UnartyProjection {
    fn new(value: Box<dyn Projection>, operator: Box<dyn UnaryFunction>) -> Self {
        let name = match operator.function_type() {
            UnaryFunctionType::Prefix => {
                format!("{} {}", operator.name(), value.name(),)
            }
            UnaryFunctionType::Postfix => {
                format!("{} {}", value.name(), operator.name(),)
            }
            UnaryFunctionType::Function => {
                format!("{}({})", operator.name(), value.name(),)
            }
        };
        Self {
            value,
            operator,
            name,
        }
    }
}

struct IsFalse {}
impl UnaryFunction for IsFalse {
    fn calculate(&self, value: SmartReference<Value>) -> SmartReference<Value> {
        Value::Bool(value.deref() == &Value::Bool(false)).into()
    }
    fn name(&self) -> &str {
        "IS FALSE"
    }
    fn function_type(&self) -> UnaryFunctionType {
        UnaryFunctionType::Postfix
    }
}

struct IsNotFalse {}
impl UnaryFunction for IsNotFalse {
    fn calculate(&self, value: SmartReference<Value>) -> SmartReference<Value> {
        Value::Bool(value.deref() != &Value::Bool(false)).into()
    }
    fn name(&self) -> &str {
        "IS NOT FALSE"
    }
    fn function_type(&self) -> UnaryFunctionType {
        UnaryFunctionType::Postfix
    }
}
struct IsTrue {}
impl UnaryFunction for IsTrue {
    fn calculate(&self, value: SmartReference<Value>) -> SmartReference<Value> {
        Value::Bool(value.deref() == &Value::Bool(true)).into()
    }
    fn name(&self) -> &str {
        "IS TRUE"
    }
    fn function_type(&self) -> UnaryFunctionType {
        UnaryFunctionType::Postfix
    }
}

struct IsNotTrue {}
impl UnaryFunction for IsNotTrue {
    fn calculate(&self, value: SmartReference<Value>) -> SmartReference<Value> {
        Value::Bool(value.deref() != &Value::Bool(true)).into()
    }
    fn name(&self) -> &str {
        "IS NOT TRUE"
    }
    fn function_type(&self) -> UnaryFunctionType {
        UnaryFunctionType::Postfix
    }
}

struct IsNull {}
impl UnaryFunction for IsNull {
    fn calculate(&self, value: SmartReference<Value>) -> SmartReference<Value> {
        Value::Bool(value.deref() == &Value::Empty).into()
    }
    fn name(&self) -> &str {
        "IS NULL"
    }
    fn function_type(&self) -> UnaryFunctionType {
        UnaryFunctionType::Postfix
    }
}

struct IsNotNull {}
impl UnaryFunction for IsNotNull {
    fn calculate(&self, value: SmartReference<Value>) -> SmartReference<Value> {
        Value::Bool(value.deref() != &Value::Empty).into()
    }
    fn name(&self) -> &str {
        "IS NOT NULL"
    }
    fn function_type(&self) -> UnaryFunctionType {
        UnaryFunctionType::Postfix
    }
}

struct Not {}
impl UnaryFunction for Not {
    fn calculate(&self, value: SmartReference<Value>) -> SmartReference<Value> {
        match value.deref() {
            Value::Empty => Value::Empty.into(),
            Value::Bool(false) => Value::Bool(true).into(),
            _ => Value::Bool(false).into(),
        }
    }
    fn name(&self) -> &str {
        "NOT"
    }
    fn function_type(&self) -> UnaryFunctionType {
        UnaryFunctionType::Prefix
    }
}

struct Negative {}
impl UnaryFunction for Negative {
    fn calculate(&self, value: SmartReference<Value>) -> SmartReference<Value> {
        match value.deref() {
            Value::Number(num) => Value::Number(-num).into(),
            _ => Value::Empty.into(),
        }
    }
    fn name(&self) -> &str {
        "-"
    }
    fn function_type(&self) -> UnaryFunctionType {
        UnaryFunctionType::Prefix
    }
}

struct PlusUnary {}
impl UnaryFunction for PlusUnary {
    fn calculate(&self, value: SmartReference<Value>) -> SmartReference<Value> {
        match value.deref() {
            Value::Number(num) => Value::Number(num.clone()).into(),
            _ => Value::Empty.into(),
        }
    }
    fn name(&self) -> &str {
        "+"
    }
    fn function_type(&self) -> UnaryFunctionType {
        UnaryFunctionType::Prefix
    }
}

struct Ceil {}
impl UnaryFunction for Ceil {
    fn calculate(&self, value: SmartReference<Value>) -> SmartReference<Value> {
        match value.deref() {
            Value::Number(num) => {
                Value::Number(num.with_scale_round(0, bigdecimal::RoundingMode::Ceiling)).into()
            }
            _ => Value::Empty.into(),
        }
    }
    fn name(&self) -> &str {
        "CEIL"
    }
    fn function_type(&self) -> UnaryFunctionType {
        UnaryFunctionType::Function
    }
}

struct Floor {}
impl UnaryFunction for Floor {
    fn calculate(&self, value: SmartReference<Value>) -> SmartReference<Value> {
        match value.deref() {
            Value::Number(num) => {
                Value::Number(num.with_scale_round(0, bigdecimal::RoundingMode::Floor)).into()
            }
            _ => Value::Empty.into(),
        }
    }
    fn name(&self) -> &str {
        "FLOOR"
    }
    fn function_type(&self) -> UnaryFunctionType {
        UnaryFunctionType::Function
    }
}

struct InProjection {
    value: Box<dyn Projection>,
    list: Vec<Box<dyn Projection>>,
    negated: bool,
    name: String,
}
impl Projection for InProjection {
    fn name(&self) -> &str {
        &self.name
    }
    fn get<'a>(&'a self, row: &'a GroupRow) -> SmartReference<'a, Value> {
        let value = self.value.get(row);
        for item in &self.list {
            let item = item.get(row);
            if item == value {
                return Value::Bool(!self.negated).into();
            }
        }
        Value::Bool(self.negated).into()
    }
}
impl InProjection {
    fn new(value: Box<dyn Projection>, list: Vec<Box<dyn Projection>>, negated: bool) -> Self {
        let in_list = list.iter().map(|t| t.name().to_string()).join(", ");
        let neg = if negated { "NOT " } else { "" };
        let name = format!("{}{} IN ({})", neg, value.name(), in_list);
        Self {
            value,
            list,
            negated,
            name,
        }
    }
}

struct InSubquery {
    value: Box<dyn Projection>,
    list: HashSet<Value>,
    negated: bool,
    name: String,
}

impl Projection for InSubquery {
    fn get<'a>(&'a self, row: &'a GroupRow) -> SmartReference<'a, Value> {
        let value = self.value.get(row);
        let contains = self.list.contains(value.deref());
        Value::Bool(self.negated != contains).into()
    }
    fn name(&self) -> &str {
        &self.name
    }
}
impl InSubquery {
    fn new(
        expr: &Expr,
        subquery: &SetExpr,
        negated: &bool,
        engine: &Engine,
        metadata: &Metadata,
    ) -> Result<Self, CvsSqlError> {
        let results = match subquery {
            SetExpr::Query(subquery) => subquery.extract(engine)?,
            SetExpr::Select(select) => select.extract(engine)?,
            _ => return Err(CvsSqlError::Unsupported(format!("IN ({})", subquery))),
        };
        if results.metadata.number_of_columns() != 1 {
            return Err(CvsSqlError::Unsupported(
                "IN (SELECT ...) with more than one column".into(),
            ));
        }
        let not = if *negated { "NOT " } else { "" };
        let name = format!("{} {}IN ({})", expr, not, subquery);
        let value = expr.convert_single(metadata, engine)?;
        let mut list = HashSet::new();
        let col = Column::from_index(0);
        for row in results.data.iter() {
            let value = row.get(&col).clone();
            list.insert(value);
        }
        Ok(Self {
            negated: *negated,
            list,
            value,
            name,
        })
    }
}

struct Between {
    value: Box<dyn Projection>,
    low: Box<dyn Projection>,
    high: Box<dyn Projection>,
    negated: bool,
    name: String,
}

impl Projection for Between {
    fn get<'a>(&'a self, row: &'a GroupRow) -> SmartReference<'a, Value> {
        let value = self.value.get(row);
        let low = self.low.get(row);
        if *value < *low {
            return Value::Bool(self.negated).into();
        }
        let high = self.high.get(row);
        if *value > *high {
            Value::Bool(self.negated).into()
        } else {
            Value::Bool(!self.negated).into()
        }
    }
    fn name(&self) -> &str {
        &self.name
    }
}
impl Between {
    fn new(
        expr: &Expr,
        low: &Expr,
        high: &Expr,
        negated: &bool,
        engine: &Engine,
        metadata: &Metadata,
    ) -> Result<Self, CvsSqlError> {
        let value = expr.convert_single(metadata, engine)?;
        let low = low.convert_single(metadata, engine)?;
        let high = high.convert_single(metadata, engine)?;
        let neg = if *negated { "NOT " } else { "" };
        let name = format!(
            "{}{} BETWEEN {} AMD {}",
            neg,
            value.name(),
            low.name(),
            high.name()
        );
        Ok(Self {
            negated: *negated,
            low,
            high,
            value,
            name,
        })
    }
}

struct SubString {
    str: Box<dyn Projection>,
    from: Option<Box<dyn Projection>>,
    size: Option<Box<dyn Projection>>,
    name: String,
}

impl Projection for SubString {
    fn get<'a>(&'a self, row: &'a GroupRow) -> SmartReference<'a, Value> {
        let str = self.str.get(row);
        let Value::Str(str) = str.deref() else {
            return Value::Empty.into();
        };
        let mut str = str.to_string();
        if let Some(from) = &self.from {
            let from = from.get(row);
            let Value::Number(from) = from.deref() else {
                return Value::Empty.into();
            };
            let Some(mut from) = from.to_usize() else {
                return Value::Empty.into();
            };
            from = from.saturating_sub(1);
            if from >= str.len() {
                return Value::Empty.into();
            }
            str = str[from..].to_string();
        }
        if let Some(size) = &self.size {
            let size = size.get(row);
            let Value::Number(size) = size.deref() else {
                return Value::Empty.into();
            };
            let Some(size) = size.to_usize() else {
                return Value::Empty.into();
            };
            if size < str.len() {
                str = str[..size].to_string();
            }
        }
        Value::Str(str.to_string()).into()
    }
    fn name(&self) -> &str {
        &self.name
    }
}

impl SubString {
    fn new(
        str: &Expr,
        from: &Option<Box<Expr>>,
        size: &Option<Box<Expr>>,
        engine: &Engine,
        metadata: &Metadata,
    ) -> Result<Self, CvsSqlError> {
        let str = str.convert_single(metadata, engine)?;
        let from = match from {
            Some(from) => Some(from.convert_single(metadata, engine)?),
            None => None,
        };
        let size = match size {
            Some(size) => Some(size.convert_single(metadata, engine)?),
            None => None,
        };
        let name = match (&from, &size) {
            (Some(from), Some(size)) => format!(
                "SUBSTRING({} FROM {} FOR {})",
                str.name(),
                from.name(),
                size.name()
            ),
            (Some(from), None) => format!("SUBSTRING({} FROM {})", str.name(), from.name()),
            (None, Some(size)) => format!("SUBSTRING({} FOR {})", str.name(), size.name()),
            (None, None) => format!("SUBSTRING({})", str.name()),
        };
        Ok(Self {
            str,
            from,
            size,
            name,
        })
    }
}

struct RegexProjection {
    value: Box<dyn Projection>,
    regex: Box<dyn Projection>,
    negated: bool,
    name: String,
}

impl Projection for RegexProjection {
    fn get<'a>(&'a self, row: &'a GroupRow) -> SmartReference<'a, Value> {
        let value = self.value.get(row);
        let regex = self.regex.get(row);
        let Ok(regex) = Regex::new(&regex.to_string()) else {
            return Value::Bool(self.negated).into();
        };
        let value = value.to_string();
        if regex.is_match(&value) {
            Value::Bool(!self.negated).into()
        } else {
            Value::Bool(self.negated).into()
        }
    }
    fn name(&self) -> &str {
        &self.name
    }
}
impl RegexProjection {
    fn new(
        expr: &Expr,
        regex: &Expr,
        negated: &bool,
        engine: &Engine,
        metadata: &Metadata,
    ) -> Result<Self, CvsSqlError> {
        let value = expr.convert_single(metadata, engine)?;
        let regex = regex.convert_single(metadata, engine)?;
        let neg = if *negated { "NOT " } else { "" };
        let name = format!("{}{} REGEXP {}", neg, value.name(), regex.name(),);
        Ok(Self {
            negated: *negated,
            regex,
            value,
            name,
        })
    }
}
impl SingleConvert for Expr {
    fn convert_single(
        &self,
        metadata: &Metadata,
        engine: &Engine,
    ) -> Result<Box<dyn Projection>, CvsSqlError> {
        match self {
            Expr::Identifier(ident) => {
                let name: Name = ident.value.to_string().into();
                name.convert_single(metadata, engine)
            }
            Expr::CompoundIdentifier(idents) => {
                let names: Vec<_> = idents.iter().map(|i| i.value.to_string()).collect();
                let name: Name = names.into();
                name.convert_single(metadata, engine)
            }
            Expr::BinaryOp { left, op, right } => {
                let left = left.convert_single(metadata, engine)?;
                let right = right.convert_single(metadata, engine)?;
                let operator: Box<dyn BinaryFunction> = match op {
                    BinaryOperator::Plus => Box::new(Plus {}),
                    BinaryOperator::Multiply => Box::new(Times {}),
                    BinaryOperator::Divide => Box::new(Divide {}),
                    BinaryOperator::Minus => Box::new(TakeAway {}),
                    BinaryOperator::Modulo => Box::new(Modulo {}),
                    BinaryOperator::StringConcat => Box::new(ConcatOperator {}),
                    BinaryOperator::Lt => Box::new(LessThen {}),
                    BinaryOperator::Gt => Box::new(GreaterThen {}),
                    BinaryOperator::Eq => Box::new(Equals {}),
                    BinaryOperator::NotEq => Box::new(NotEquals {}),
                    BinaryOperator::GtEq => Box::new(GreaterThenEq {}),
                    BinaryOperator::LtEq => Box::new(LessThenEq {}),
                    BinaryOperator::And => Box::new(AndBinaryFunction {}),
                    BinaryOperator::Or => Box::new(OrBinaryFunction {}),
                    BinaryOperator::Xor => Box::new(XorBinaryFunction {}),
                    _ => {
                        return Err(CvsSqlError::Unsupported(format!("Operator: {}", op)));
                    }
                };
                Ok(Box::new(BinaryProjection::new(left, right, operator)))
            }
            Expr::Value(val) => {
                let name = self.to_string();
                match &val.value {
                    AstValue::Number(num, _) => {
                        let value = Value::Number(num.clone());
                        Ok(Box::new(ValueProjection { value, name }))
                    }
                    AstValue::Boolean(b) => {
                        let value = Value::Bool(*b);
                        Ok(Box::new(ValueProjection { value, name }))
                    }
                    AstValue::SingleQuotedString(s) => {
                        let value = s.as_str().into();
                        Ok(Box::new(ValueProjection { value, name }))
                    }
                    AstValue::Null => Ok(Box::new(ValueProjection {
                        value: Value::Empty,
                        name,
                    })),
                    _ => Err(CvsSqlError::Unsupported(format!(
                        "Select literal value {}",
                        self
                    ))),
                }
            }
            Expr::IsFalse(val) => {
                let value = val.convert_single(metadata, engine)?;
                let operator = Box::new(IsFalse {});
                Ok(Box::new(UnartyProjection::new(value, operator)))
            }
            Expr::IsNotFalse(val) => {
                let value = val.convert_single(metadata, engine)?;
                let operator = Box::new(IsNotFalse {});
                Ok(Box::new(UnartyProjection::new(value, operator)))
            }
            Expr::IsTrue(val) => {
                let value = val.convert_single(metadata, engine)?;
                let operator = Box::new(IsTrue {});
                Ok(Box::new(UnartyProjection::new(value, operator)))
            }
            Expr::IsNotTrue(val) => {
                let value = val.convert_single(metadata, engine)?;
                let operator = Box::new(IsNotTrue {});
                Ok(Box::new(UnartyProjection::new(value, operator)))
            }
            Expr::IsNull(val) => {
                let value = val.convert_single(metadata, engine)?;
                let operator = Box::new(IsNull {});
                Ok(Box::new(UnartyProjection::new(value, operator)))
            }
            Expr::IsNotNull(val) => {
                let value = val.convert_single(metadata, engine)?;
                let operator = Box::new(IsNotNull {});
                Ok(Box::new(UnartyProjection::new(value, operator)))
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let value = expr.convert_single(metadata, engine)?;
                let mut items = Vec::new();
                for item in list {
                    items.push(item.convert_single(metadata, engine)?);
                }

                Ok(Box::new(InProjection::new(value, items, *negated)))
            }
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let expr = InSubquery::new(expr, subquery, negated, engine, metadata)?;
                Ok(Box::new(expr))
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let expr = Between::new(expr, low, high, negated, engine, metadata)?;
                Ok(Box::new(expr))
            }
            Expr::RLike {
                negated,
                expr,
                pattern,
                regexp: _,
            } => {
                let expr = RegexProjection::new(expr, pattern, negated, engine, metadata)?;
                Ok(Box::new(expr))
            }
            Expr::SimilarTo {
                negated,
                expr,
                pattern,
                escape_char: _,
            } => {
                let expr = RegexProjection::new(expr, pattern, negated, engine, metadata)?;
                Ok(Box::new(expr))
            }
            Expr::UnaryOp { op, expr } => {
                let operator: Box<dyn UnaryFunction> = match op {
                    UnaryOperator::Minus => Box::new(Negative {}),
                    UnaryOperator::Plus => Box::new(PlusUnary {}),
                    UnaryOperator::Not => Box::new(Not {}),
                    _ => return Err(CvsSqlError::Unsupported(format!("Operator: {}", op))),
                };
                let value = expr.convert_single(metadata, engine)?;
                Ok(Box::new(UnartyProjection::new(value, operator)))
            }
            Expr::Cast {
                kind: _,
                expr,
                data_type,
                format,
            } => {
                if format.is_some() {
                    return Err(CvsSqlError::Unsupported("CAST with format".to_string()));
                }
                let value = expr.convert_single(metadata, engine)?;
                create_cast(data_type, value)
            }
            Expr::Convert {
                is_try: _,
                expr,
                data_type,
                charset,
                target_before_value: _,
                styles: _,
            } => {
                if charset.is_some() {
                    return Err(CvsSqlError::Unsupported("CONVERT with charset".to_string()));
                };
                let Some(data_type) = data_type else {
                    return Err(CvsSqlError::Unsupported("CONVERT with charset".to_string()));
                };
                let value = expr.convert_single(metadata, engine)?;
                create_cast(data_type, value)
            }
            Expr::Extract {
                field,
                syntax: _,
                expr,
            } => {
                let value = expr.convert_single(metadata, engine)?;
                create_extract(field, value)
            }
            Expr::Ceil { expr, field } => {
                match field {
                    CeilFloorKind::DateTimeField(DateTimeField::NoDateTime) => {}
                    _ => {
                        return Err(CvsSqlError::Unsupported(
                            "CEIL with two arguments".to_string(),
                        ));
                    }
                }

                let value = expr.convert_single(metadata, engine)?;
                let operator = Box::new(Ceil {});
                Ok(Box::new(UnartyProjection::new(value, operator)))
            }
            Expr::Floor { expr, field } => {
                match field {
                    CeilFloorKind::DateTimeField(DateTimeField::NoDateTime) => {}
                    _ => {
                        return Err(CvsSqlError::Unsupported(
                            "CEIL with two arguments".to_string(),
                        ));
                    }
                }

                let value = expr.convert_single(metadata, engine)?;
                let operator = Box::new(Floor {});
                Ok(Box::new(UnartyProjection::new(value, operator)))
            }
            Expr::Position { expr, r#in } => {
                let sub_str = expr.convert_single(metadata, engine)?;
                let str = r#in.convert_single(metadata, engine)?;
                let func = Position::new(str, sub_str);
                Ok(Box::new(func))
            }
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
                special: _,
                shorthand: _,
            } => {
                let sub = SubString::new(expr, substring_from, substring_for, engine, metadata)?;
                Ok(Box::new(sub))
            }
            Expr::Function(func) => func.convert_single(metadata, engine),
            Expr::Case {
                operand,
                conditions,
                else_result,
            } => new_case(operand, conditions, else_result, metadata, engine),

            _ => Err(CvsSqlError::Unsupported(format!(
                "Select expression like {}",
                self
            ))),
        }
    }
}

impl SingleConvert for Name {
    fn convert_single(
        &self,
        metadata: &Metadata,
        _: &Engine,
    ) -> Result<Box<dyn Projection>, CvsSqlError> {
        let column = metadata.column_index(self)?;
        let projection = Box::new(ColumnProjection {
            column: column.clone(),
            column_name: self.short_name().to_string(),
        });
        Ok(projection)
    }
}

struct Position {
    str: Box<dyn Projection>,
    sub_str: Box<dyn Projection>,
    name: String,
}
impl Projection for Position {
    fn get<'a>(&'a self, row: &'a GroupRow) -> SmartReference<'a, Value> {
        let str = self.str.get(row);
        let sub_str = self.sub_str.get(row);

        let Value::Str(sub_str) = sub_str.deref() else {
            return Value::Empty.into();
        };
        let Value::Str(str) = str.deref() else {
            return Value::Empty.into();
        };
        let num = str.find(sub_str).map(|f| f + 1).unwrap_or(0);
        let num = match BigDecimal::from_usize(num) {
            None => Value::Empty,
            Some(num) => Value::Number(num),
        };

        num.into()
    }
    fn name(&self) -> &str {
        &self.name
    }
}
impl Position {
    fn new(str: Box<dyn Projection>, sub_str: Box<dyn Projection>) -> Self {
        let name = format!("POSITION({} IN {})", sub_str.name(), sub_str.name());
        Self { name, str, sub_str }
    }
}

struct Case {
    leavs: Vec<(Box<dyn Projection>, Box<dyn Projection>)>,
    default: Option<Box<dyn Projection>>,
    name: String,
}
fn new_case(
    operand: &Option<Box<Expr>>,
    conditions: &[CaseWhen],
    else_result: &Option<Box<Expr>>,
    metadata: &Metadata,
    engine: &Engine,
) -> Result<Box<dyn Projection>, CvsSqlError> {
    if operand.is_some() {
        return Err(CvsSqlError::Unsupported("CASE with Operand".into()));
    }
    if conditions.is_empty() {
        return Err(CvsSqlError::Unsupported("CASE without conditions".into()));
    }
    let mut leavs = Vec::new();
    let mut name = "CASE ".to_string();
    for condition in conditions.iter() {
        let result = condition.result.convert_single(metadata, engine)?;
        let condition = condition.condition.convert_single(metadata, engine)?;
        name = format!("{} WHEN {} THEN {} ", name, condition.name(), result.name());
        leavs.push((condition, result));
    }
    let default = else_result
        .iter()
        .map(|e| e.convert_single(metadata, engine))
        .next()
        .transpose()?;
    name = match &default {
        Some(default) => format!("{} ELSE {}", name, default.name()),
        None => name,
    };
    name = format!("{} END", name);
    Ok(Box::new(Case {
        leavs,
        default,
        name,
    }))
}
impl Projection for Case {
    fn get<'a>(&'a self, row: &'a GroupRow) -> SmartReference<'a, Value> {
        for (condition, result) in &self.leavs {
            if condition.get(row).deref() == &Value::Bool(true) {
                return result.get(row);
            }
        }
        match &self.default {
            Some(default) => default.get(row),
            None => Value::Empty.into(),
        }
    }
    fn name(&self) -> &str {
        &self.name
    }
}
