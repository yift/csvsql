use sqlparser::ast::{BinaryOperator, Expr, Query, SelectItem, WildcardAdditionalOptions};

use crate::engine::Engine;
use crate::error::CdvSqlError;
use crate::extractor::Extractor;
use crate::results::ResultName;
use crate::util::SmartReference;
use crate::{
    results::{Column, ColumnName, ResultSet},
    value::Value,
};
use itertools::Itertools;
use sqlparser::ast::Value as AstValue;
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::rc::Rc;

pub trait Projection {
    fn get<'a>(&'a self, results: &'a dyn ResultSet) -> SmartReference<'a, Value>;
    fn name(&self) -> SmartReference<'_, ColumnName>;
}
struct ColumnProjection {
    column: Column,
    column_name: ColumnName,
}
impl Projection for ColumnProjection {
    fn get<'a>(&'a self, results: &'a dyn ResultSet) -> SmartReference<'a, Value> {
        results.get(&self.column)
    }
    fn name(&self) -> SmartReference<'_, ColumnName> {
        SmartReference::Borrowed(&self.column_name)
    }
}

struct ResultsWithProjections {
    projections: Vec<Box<dyn Projection>>,
    names: HashMap<String, Vec<Column>>,
    results: Box<dyn ResultSet>,
}

impl ResultSet for ResultsWithProjections {
    fn number_of_columns(&self) -> usize {
        self.projections.len()
    }
    fn result_name(&self) -> Option<&Rc<ResultName>> {
        self.results.result_name()
    }
    fn column_name(&self, column: &Column) -> Option<ColumnName> {
        self.projections
            .get(column.get_index())
            .map(|projection| projection.name().clone())
    }
    fn column_index(&self, name: &ColumnName) -> Option<Column> {
        if let Some(projections) = self.names.get(name.name()) {
            for idx in projections {
                if self.projections[idx.get_index()]
                    .name()
                    .parent()
                    .matches(name.parent())
                {
                    return Some(idx.clone());
                }
            }
        }
        None
    }

    fn get(&self, column: &Column) -> SmartReference<Value> {
        self.projections
            .get(column.get_index())
            .map(|p| p.get(&*self.results))
            .unwrap_or(Value::Empty.into())
    }
    fn next_if_possible(&mut self) -> bool {
        self.results.next_if_possible()
    }
    fn revert(&mut self) {
        self.results.revert();
    }
}

pub fn make_projection(
    engine: &Engine,
    parent: Box<dyn ResultSet>,
    items: &[SelectItem],
) -> Result<Box<dyn ResultSet>, CdvSqlError> {
    let mut projections = Vec::new();
    for item in items {
        let mut items = item.convert(&*parent, engine)?;
        projections.append(&mut items);
    }
    let mut names: HashMap<String, Vec<Column>> = HashMap::new();
    for (idx, p) in projections.iter().enumerate() {
        names
            .entry(p.name().name().to_string())
            .and_modify(|lst| lst.push(Column::from_index(idx)))
            .or_insert(vec![Column::from_index(idx)]);
    }
    Ok(Box::new(ResultsWithProjections {
        projections,
        names,
        results: parent,
    }))
}
trait Convert {
    fn convert(
        &self,
        parent: &dyn ResultSet,
        engine: &Engine,
    ) -> Result<Vec<Box<dyn Projection>>, CdvSqlError>;
}
impl Convert for SelectItem {
    fn convert(
        &self,
        parent: &dyn ResultSet,
        engine: &Engine,
    ) -> Result<Vec<Box<dyn Projection>>, CdvSqlError> {
        match self {
            SelectItem::Wildcard(options) => options.convert(parent, engine),
            SelectItem::UnnamedExpr(exp) => exp.convert(parent, engine),
            SelectItem::ExprWithAlias { expr, alias } => {
                let data = expr.convert_single(parent, engine)?;
                let alias = ColumnName::simple(&alias.value);
                Ok(vec![Box::new(AliasProjection { data, alias })])
            }
            _ => Err(CdvSqlError::ToDo(format!("Select {}", self))),
        }
    }
}
impl Convert for WildcardAdditionalOptions {
    fn convert(
        &self,
        parent: &dyn ResultSet,
        _: &Engine,
    ) -> Result<Vec<Box<dyn Projection>>, CdvSqlError> {
        if self.opt_ilike.is_some() {
            return Err(CdvSqlError::Unsupported("Select * ILIKE".into()));
        }
        if self.opt_exclude.is_some() {
            return Err(CdvSqlError::Unsupported("Select * EXCLUDE".into()));
        }
        if self.opt_except.is_some() {
            return Err(CdvSqlError::Unsupported("Select * EXCEPT".into()));
        }
        if self.opt_replace.is_some() {
            return Err(CdvSqlError::Unsupported("Select * REPLACE".into()));
        }
        if self.opt_rename.is_some() {
            return Err(CdvSqlError::Unsupported("Select * RENAME".into()));
        }
        let mut projections: Vec<Box<dyn Projection>> = Vec::new();
        for column in parent.columns() {
            let Some(column_name) = parent.column_name(&column) else {
                return Err(CdvSqlError::Unsupported(
                    "Select * with unnamed column".into(),
                ));
            };
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
        parent: &dyn ResultSet,
        engine: &Engine,
    ) -> Result<Box<dyn Projection>, CdvSqlError>;
}

trait BinaryFunction {
    fn calculate(
        &self,
        left: SmartReference<Value>,
        right: SmartReference<Value>,
    ) -> SmartReference<Value>;
    fn name(&self) -> &str;
    fn is_operator(&self) -> bool;
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
    fn is_operator(&self) -> bool {
        true
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
    fn is_operator(&self) -> bool {
        true
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
    fn is_operator(&self) -> bool {
        true
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
    fn is_operator(&self) -> bool {
        true
    }
}
struct Modulu {}
impl BinaryFunction for Modulu {
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
    fn is_operator(&self) -> bool {
        true
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
    fn is_operator(&self) -> bool {
        true
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
    fn is_operator(&self) -> bool {
        true
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
    fn is_operator(&self) -> bool {
        true
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
    fn is_operator(&self) -> bool {
        true
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
    fn is_operator(&self) -> bool {
        true
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
    fn is_operator(&self) -> bool {
        true
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
    fn is_operator(&self) -> bool {
        true
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
    fn is_operator(&self) -> bool {
        true
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
    fn is_operator(&self) -> bool {
        true
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
    fn is_operator(&self) -> bool {
        true
    }
}

struct AliasProjection {
    data: Box<dyn Projection>,
    alias: ColumnName,
}
impl Projection for AliasProjection {
    fn get<'a>(&'a self, results: &'a dyn ResultSet) -> SmartReference<'a, Value> {
        self.data.get(results)
    }
    fn name(&self) -> SmartReference<'_, ColumnName> {
        SmartReference::Borrowed(&self.alias)
    }
}
struct BinaryProjection {
    left: Box<dyn Projection>,
    right: Box<dyn Projection>,
    operator: Box<dyn BinaryFunction>,
}
impl Projection for BinaryProjection {
    fn name(&self) -> SmartReference<ColumnName> {
        let name = if self.operator.is_operator() {
            format!(
                "{} {} {}",
                self.left.name(),
                self.operator.name(),
                self.right.name()
            )
        } else {
            format!(
                "{}({}, {})",
                self.operator.name(),
                self.left.name(),
                self.right.name()
            )
        };
        ColumnName::simple(&name).into()
    }
    fn get<'a>(&'a self, results: &'a dyn ResultSet) -> SmartReference<'a, Value> {
        let left = self.left.get(results);
        let right = self.right.get(results);
        self.operator.calculate(left, right)
    }
}

impl<T: SingleConvert> Convert for T {
    fn convert(
        &self,
        parent: &dyn ResultSet,
        engine: &Engine,
    ) -> Result<Vec<Box<dyn Projection>>, CdvSqlError> {
        let result = self.convert_single(parent, engine)?;
        Ok(vec![result])
    }
}
struct ValueProjection {
    value: Value,
    name: String,
}
impl Projection for ValueProjection {
    fn get<'a>(&'a self, _: &'a dyn ResultSet) -> SmartReference<'a, Value> {
        SmartReference::Borrowed(&self.value)
    }
    fn name(&self) -> SmartReference<'_, ColumnName> {
        let name = ColumnName::simple(&self.name);
        name.into()
    }
}

trait UnaryFunction {
    fn calculate(&self, value: SmartReference<Value>) -> SmartReference<Value>;
    fn name(&self) -> &str;
    fn function_type(&self) -> UnaryFunctionType;
}

enum UnaryFunctionType {
    Postfix,
    Function,
}
struct UnartyProjection {
    value: Box<dyn Projection>,
    operator: Box<dyn UnaryFunction>,
}

impl Projection for UnartyProjection {
    fn get<'a>(&'a self, results: &'a dyn ResultSet) -> SmartReference<'a, Value> {
        let value = self.value.get(results);
        self.operator.calculate(value)
    }
    fn name(&self) -> SmartReference<'_, ColumnName> {
        let name = match self.operator.function_type() {
            UnaryFunctionType::Postfix => {
                format!("{} {}", self.value.name(), self.operator.name(),)
            }
            UnaryFunctionType::Function => {
                format!("{}({})", self.operator.name(), self.value.name(),)
            }
        };
        ColumnName::simple(&name).into()
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

struct InProjection {
    value: Box<dyn Projection>,
    list: Vec<Box<dyn Projection>>,
    negated: bool,
}
impl Projection for InProjection {
    fn name(&self) -> SmartReference<ColumnName> {
        let list = self.list.iter().map(|t| format!("{}", t.name())).join(", ");
        let neg = if self.negated { "NOT " } else { "" };
        let name = format!("{}{} IN ({})", neg, self.value.name(), list);
        ColumnName::simple(&name).into()
    }
    fn get<'a>(&'a self, results: &'a dyn ResultSet) -> SmartReference<'a, Value> {
        let value = self.value.get(results);
        for item in &self.list {
            let item = item.get(results);
            if item == value {
                return Value::Bool(!self.negated).into();
            }
        }
        Value::Bool(self.negated).into()
    }
}

struct InSubquery {
    value: Box<dyn Projection>,
    list: HashSet<Value>,
    negated: bool,
    name: ColumnName,
}

impl Projection for InSubquery {
    fn get<'a>(&'a self, results: &'a dyn ResultSet) -> SmartReference<'a, Value> {
        let value = self.value.get(results);
        let contains = self.list.contains(value.deref());
        Value::Bool(self.negated != contains).into()
    }
    fn name(&self) -> SmartReference<'_, ColumnName> {
        (&self.name).into()
    }
}
impl InSubquery {
    fn new(
        expr: &Expr,
        subquery: &Query,
        negated: &bool,
        engine: &Engine,
        parent: &dyn ResultSet,
    ) -> Result<Self, CdvSqlError> {
        let mut results = subquery.extract(engine)?;
        if results.number_of_columns() != 1 {
            return Err(CdvSqlError::Unsupported(
                "IN (SELECT ...) with more than one column".into(),
            ));
        }
        let not = if *negated { "NOT " } else { "" };
        let name = format!("{} {}IN ({})", expr, not, subquery);
        let value = expr.convert_single(parent, engine)?;
        let mut list = HashSet::new();
        let col = Column::from_index(0);
        while results.next_if_possible() {
            let value = results.get(&col);
            let value = value.extract();
            list.insert(value);
        }
        Ok(Self {
            negated: *negated,
            list,
            value,
            name: ColumnName::simple(&name),
        })
    }
}

impl SingleConvert for Expr {
    fn convert_single(
        &self,
        parent: &dyn ResultSet,
        engine: &Engine,
    ) -> Result<Box<dyn Projection>, CdvSqlError> {
        match self {
            Expr::Identifier(ident) => {
                let name = ColumnName::simple(&ident.value);
                name.convert_single(parent, engine)
            }
            Expr::CompoundIdentifier(idents) => {
                let mut root = ResultName::root();
                let mut names = idents.iter().peekable();
                while let Some(name) = names.next() {
                    if names.peek().is_none() {
                        let name = ColumnName::new(&Rc::new(root), &name.value);

                        return name.convert_single(parent, engine);
                    } else {
                        root = root.append(&name.value);
                    }
                }
                Err(CdvSqlError::NoSelect)
            }
            Expr::BinaryOp { left, op, right } => {
                let left = left.convert_single(parent, engine)?;
                let right = right.convert_single(parent, engine)?;
                let operator: Box<dyn BinaryFunction> = match op {
                    BinaryOperator::Plus => Box::new(Plus {}),
                    BinaryOperator::Multiply => Box::new(Times {}),
                    BinaryOperator::Divide => Box::new(Divide {}),
                    BinaryOperator::Minus => Box::new(TakeAway {}),
                    BinaryOperator::Modulo => Box::new(Modulu {}),
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
                        return Err(CdvSqlError::Unsupported(format!("Operator: {}", op)));
                    }
                };
                Ok(Box::new(BinaryProjection {
                    left,
                    right,
                    operator,
                }))
            }
            Expr::Value(val) => {
                let name = self.to_string();
                match val {
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
                    _ => Err(CdvSqlError::ToDo(format!("Select literal value {}", self))),
                }
            }
            Expr::IsFalse(val) => {
                let value = val.convert_single(parent, engine)?;
                let operator = Box::new(IsFalse {});
                Ok(Box::new(UnartyProjection { value, operator }))
            }
            Expr::IsNotFalse(val) => {
                let value = val.convert_single(parent, engine)?;
                let operator = Box::new(IsNotFalse {});
                Ok(Box::new(UnartyProjection { value, operator }))
            }
            Expr::IsTrue(val) => {
                let value = val.convert_single(parent, engine)?;
                let operator = Box::new(IsTrue {});
                Ok(Box::new(UnartyProjection { value, operator }))
            }
            Expr::IsNotTrue(val) => {
                let value = val.convert_single(parent, engine)?;
                let operator = Box::new(IsNotTrue {});
                Ok(Box::new(UnartyProjection { value, operator }))
            }
            Expr::IsNull(val) => {
                let value = val.convert_single(parent, engine)?;
                let operator = Box::new(IsNull {});
                Ok(Box::new(UnartyProjection { value, operator }))
            }
            Expr::IsNotNull(val) => {
                let value = val.convert_single(parent, engine)?;
                let operator = Box::new(IsNotNull {});
                Ok(Box::new(UnartyProjection { value, operator }))
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let value = expr.convert_single(parent, engine)?;
                let mut items = Vec::new();
                for item in list {
                    items.push(item.convert_single(parent, engine)?);
                }

                Ok(Box::new(InProjection {
                    value,
                    list: items,
                    negated: *negated,
                }))
            }
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let expr = InSubquery::new(expr, subquery, negated, engine, parent)?;
                Ok(Box::new(expr))
            }
            _ => Err(CdvSqlError::ToDo(format!(
                "Select expression like {}",
                self
            ))),
        }
    }
}
impl SingleConvert for ColumnName {
    fn convert_single(
        &self,
        parent: &dyn ResultSet,
        _: &Engine,
    ) -> Result<Box<dyn Projection>, CdvSqlError> {
        let Some(column) = parent.column_index(self) else {
            return Err(CdvSqlError::NoSuchColumn(self.name().into()));
        };
        let Some(column_name) = parent.column_name(&column) else {
            return Err(CdvSqlError::NoSuchColumn(self.name().into()));
        };
        let projection = Box::new(ColumnProjection {
            column: column.clone(),
            column_name,
        });
        Ok(projection)
    }
}
