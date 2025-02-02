use std::ops::Deref;
use std::str::FromStr;

use bigdecimal::BigDecimal;
use bigdecimal::Zero;
use chrono::NaiveDate;
use chrono::NaiveDateTime;
use chrono::NaiveTime;
use sqlparser::ast::DataType;

use crate::error::CvsSqlError;
use crate::projections::Projection;
use crate::results::ResultSet;
use crate::util::SmartReference;
use crate::value::Value;
struct Cast {
    to_cast: Box<dyn Projection>,
    data_type: AvailableDataTypes,
    name: String,
}
impl Projection for Cast {
    fn get<'a>(&'a self, results: &'a dyn ResultSet) -> SmartReference<'a, Value> {
        let value = self.to_cast.get(results);
        self.data_type.convert(value)
    }
    fn name(&self) -> &str {
        &self.name
    }
}

pub fn create_cast(
    data_type: &DataType,
    to_cast: Box<dyn Projection>,
) -> Result<Box<dyn Projection>, CvsSqlError> {
    let data_type = match data_type {
        DataType::Character(_)
        | DataType::Char(_)
        | DataType::CharacterVarying(_)
        | DataType::CharVarying(_)
        | DataType::Varchar(_)
        | DataType::Nvarchar(_)
        | DataType::String(_)
        | DataType::FixedString(_)
        | DataType::LongText
        | DataType::MediumText
        | DataType::TinyText
        | DataType::Text
        | DataType::CharacterLargeObject(_)
        | DataType::CharLargeObject(_)
        | DataType::Clob(_) => AvailableDataTypes::Str,

        DataType::Numeric(_)
        | DataType::Decimal(_)
        | DataType::BigNumeric(_)
        | DataType::BigDecimal(_)
        | DataType::Dec(_)
        | DataType::Float(_)
        | DataType::TinyInt(_)
        | DataType::UnsignedTinyInt(_)
        | DataType::Int2(_)
        | DataType::UnsignedInt2(_)
        | DataType::SmallInt(_)
        | DataType::UnsignedSmallInt(_)
        | DataType::MediumInt(_)
        | DataType::UnsignedMediumInt(_)
        | DataType::Int(_)
        | DataType::Int4(_)
        | DataType::Int8(_)
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Int128
        | DataType::Int256
        | DataType::Integer(_)
        | DataType::UnsignedInt(_)
        | DataType::UnsignedInt4(_)
        | DataType::UnsignedInteger(_)
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::UInt128
        | DataType::UInt256
        | DataType::BigInt(_)
        | DataType::UnsignedBigInt(_)
        | DataType::UnsignedInt8(_)
        | DataType::Float4
        | DataType::Float32
        | DataType::Float64
        | DataType::Real
        | DataType::Float8
        | DataType::Double
        | DataType::DoublePrecision => AvailableDataTypes::Number,

        DataType::Bool | DataType::Boolean => AvailableDataTypes::Bool,

        DataType::Date | DataType::Date32 => AvailableDataTypes::Date,

        DataType::Time(_, _)
        | DataType::Datetime(_)
        | DataType::Datetime64(_, _)
        | DataType::Timestamp(_, _) => AvailableDataTypes::Timestamp,

        _ => return Err(CvsSqlError::Unsupported(format!("CAST to {}", data_type))),
    };
    let name = format!("TRY_CAST({} AS {})", to_cast.name(), data_type.name());

    Ok(Box::new(Cast {
        to_cast,
        data_type,
        name,
    }))
}

enum AvailableDataTypes {
    Str,
    Number,
    Bool,
    Timestamp,
    Date,
}
impl AvailableDataTypes {
    fn name(&self) -> &str {
        match self {
            AvailableDataTypes::Bool => "BOOL",
            AvailableDataTypes::Str => "TEXT",
            AvailableDataTypes::Number => "DECIMAL",
            AvailableDataTypes::Date => "DATE",
            AvailableDataTypes::Timestamp => "TIMESTAMP",
        }
    }

    fn convert<'a>(&self, value: SmartReference<'a, Value>) -> SmartReference<'a, Value> {
        match self {
            AvailableDataTypes::Str => convert_to_string(value),
            AvailableDataTypes::Number => convert_to_number(value),
            AvailableDataTypes::Bool => convert_to_bool(value),
            AvailableDataTypes::Date => convert_to_date(value),
            AvailableDataTypes::Timestamp => convert_to_timestamp(value),
        }
    }
}
fn convert_to_string(value: SmartReference<'_, Value>) -> SmartReference<'_, Value> {
    match value.deref() {
        Value::Empty | Value::Str(_) => value,
        _ => Value::Str(format!("{}", value)).into(),
    }
}
fn convert_to_number(value: SmartReference<'_, Value>) -> SmartReference<'_, Value> {
    match value.deref() {
        Value::Empty | Value::Number(_) => value,
        Value::Str(str) => match BigDecimal::from_str(str) {
            Ok(num) => Value::Number(num).into(),
            _ => Value::Empty.into(),
        },
        _ => Value::Empty.into(),
    }
}
fn convert_to_bool(value: SmartReference<'_, Value>) -> SmartReference<'_, Value> {
    match value.deref() {
        Value::Empty | Value::Bool(_) => value,
        Value::Number(num) => Value::Bool(!num.is_zero()).into(),
        Value::Str(str) => match str.to_uppercase().as_str() {
            "TRUE" | "T" | "Y" | "YES" | "1" => Value::Bool(true).into(),
            "FALSE" | "F" | "N" | "NO" | "0" => Value::Bool(false).into(),
            _ => Value::Empty.into(),
        },
        _ => Value::Empty.into(),
    }
}
fn convert_to_date(value: SmartReference<'_, Value>) -> SmartReference<'_, Value> {
    match value.deref() {
        Value::Empty | Value::Date(_) => value,
        Value::Timestamp(t) => Value::Date(t.date()).into(),
        Value::Str(str) => match NaiveDate::parse_from_str(str, "%Y-%m-%d") {
            Ok(date) => Value::Date(date).into(),
            _ => Value::Empty.into(),
        },
        _ => Value::Empty.into(),
    }
}
fn convert_to_timestamp(value: SmartReference<'_, Value>) -> SmartReference<'_, Value> {
    match value.deref() {
        Value::Empty | Value::Timestamp(_) => value,
        Value::Date(d) => Value::Timestamp(d.and_time(NaiveTime::default())).into(),
        Value::Str(str) => match NaiveDateTime::parse_from_str(str, "%Y-%m-%d %H:%M:%S%.f") {
            Ok(date) => Value::Timestamp(date).into(),
            _ => Value::Empty.into(),
        },
        _ => Value::Empty.into(),
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use bigdecimal::FromPrimitive;

    #[test]
    fn convert_str_return_string() {
        let value = Value::Number(BigDecimal::from_i16(101).unwrap());

        let casted = AvailableDataTypes::Str.convert(value.into());

        assert_eq!(casted, Value::Str("101".into()).into());
    }

    #[test]
    fn convert_number_return_number() {
        let value = Value::Str("100".into());

        let casted = AvailableDataTypes::Number.convert(value.into());

        assert_eq!(
            casted,
            Value::Number(BigDecimal::from_i16(100).unwrap()).into()
        );
    }

    #[test]
    fn convert_bool_return_bool() {
        let value = Value::Str("Yes".into());

        let casted = AvailableDataTypes::Bool.convert(value.into());

        assert_eq!(casted, Value::Bool(true).into());
    }

    #[test]
    fn convert_date_return_date() {
        let value = Value::Str("2024-05-22".into());

        let casted = AvailableDataTypes::Date.convert(value.into());

        assert_eq!(
            casted,
            Value::Date(NaiveDate::from_str("2024-05-22").unwrap()).into()
        );
    }

    #[test]
    fn convert_datetime_return_datetime() {
        let value = Value::Str("2024-05-22 11:11:11".into());

        let casted = AvailableDataTypes::Timestamp.convert(value.into());

        assert_eq!(
            casted,
            Value::Timestamp(NaiveDateTime::from_str("2024-05-22T11:11:11").unwrap()).into()
        );
    }

    #[test]
    fn convert_to_string_return_empty_for_empty() {
        let casted = convert_to_string(Value::Empty.into());

        assert_eq!(casted.deref(), &Value::Empty,);
    }

    #[test]
    fn convert_to_string_return_string_value() {
        let value = Value::Str("test".into());
        let casted = convert_to_string(SmartReference::Borrowed(&value));

        assert_eq!(casted.deref(), &value,);
    }

    #[test]
    fn convert_to_string_return_format_value() {
        let value = Value::Bool(true);
        let casted = convert_to_string(SmartReference::Borrowed(&value));

        assert_eq!(casted.deref(), &Value::Str("TRUE".into()),);
    }

    #[test]
    fn convert_to_number_return_empty() {
        let casted = convert_to_number(Value::Empty.into());

        assert_eq!(casted.deref(), &Value::Empty,);
    }

    #[test]
    fn convert_to_number_return_number_value() {
        let value = Value::Number(BigDecimal::from_i16(12).unwrap());
        let casted = convert_to_number(SmartReference::Borrowed(&value));

        assert_eq!(casted.deref(), &value,);
    }

    #[test]
    fn convert_to_number_empty_for_no_number() {
        let value = Value::Bool(false);
        let casted = convert_to_number(SmartReference::Borrowed(&value));

        assert_eq!(casted.deref(), &Value::Empty,);
    }

    #[test]
    fn convert_to_number_empty_for_invalid() {
        let value = Value::Str("test".into());
        let casted = convert_to_number(SmartReference::Borrowed(&value));

        assert_eq!(casted.deref(), &Value::Empty,);
    }

    #[test]
    fn convert_to_number_number_to_numeric_string() {
        let value = Value::Str("1.32".into());
        let casted = convert_to_number(SmartReference::Borrowed(&value));

        assert_eq!(
            casted.deref(),
            &Value::Number(BigDecimal::from_str("1.32").unwrap()),
        );
    }

    #[test]
    fn convert_to_bool_return_empty() {
        let casted = convert_to_bool(Value::Empty.into());

        assert_eq!(casted.deref(), &Value::Empty,);
    }

    #[test]
    fn convert_to_bool_return_bool_value() {
        let value = Value::Bool(false);
        let casted = convert_to_bool(SmartReference::Borrowed(&value));

        assert_eq!(casted.deref(), &value,);
    }

    #[test]
    fn convert_to_bool_return_bool_for_non_zero_number() {
        let value = Value::Number(BigDecimal::from_i16(12).unwrap());
        let casted = convert_to_bool(SmartReference::Borrowed(&value));

        assert_eq!(casted.deref(), &Value::Bool(true),);
    }

    #[test]
    fn convert_to_bool_return_bool_for_zero_number() {
        let value = Value::Number(BigDecimal::zero());
        let casted = convert_to_bool(SmartReference::Borrowed(&value));

        assert_eq!(casted.deref(), &Value::Bool(false),);
    }

    #[test]
    fn convert_to_bool_return_empty_for_string() {
        let value = Value::Str("test".into());
        let casted = convert_to_bool(SmartReference::Borrowed(&value));

        assert_eq!(casted.deref(), &Value::Empty,);
    }

    #[test]
    fn convert_to_bool_return_empty_for_date() {
        let value = Value::Date(NaiveDate::from_str("2022-04-20").unwrap());
        let casted = convert_to_bool(SmartReference::Borrowed(&value));

        assert_eq!(casted.deref(), &Value::Empty,);
    }

    #[test]
    fn convert_to_bool_return_correct_values_from_true_strings() {
        let strs = vec!["t", "True", "y", "YES", "1"];
        for str in strs {
            let value = Value::Str(str.into());
            let casted = convert_to_bool(SmartReference::Borrowed(&value));

            assert_eq!(casted.deref(), &Value::Bool(true),);
        }
    }

    #[test]
    fn convert_to_bool_return_correct_values_from_false_strings() {
        let strs = vec!["f", "false", "n", "no", "0"];
        for str in strs {
            let value = Value::Str(str.into());
            let casted = convert_to_bool(SmartReference::Borrowed(&value));

            assert_eq!(casted.deref(), &Value::Bool(false),);
        }
    }

    #[test]
    fn convert_to_date_return_empty() {
        let casted = convert_to_date(Value::Empty.into());

        assert_eq!(casted.deref(), &Value::Empty,);
    }

    #[test]
    fn convert_to_date_return_date_value() {
        let value = Value::Date(NaiveDate::from_str("1984-11-02").unwrap());
        let casted = convert_to_date(SmartReference::Borrowed(&value));

        assert_eq!(casted.deref(), &value,);
    }

    #[test]
    fn convert_to_date_return_date_from_timestamp_value() {
        let value = Value::Timestamp(NaiveDateTime::from_str("1984-11-02T08:10:21").unwrap());
        let casted = convert_to_date(SmartReference::Borrowed(&value));

        assert_eq!(
            casted.deref(),
            &Value::Date(NaiveDate::from_str("1984-11-02").unwrap()),
        );
    }

    #[test]
    fn convert_to_date_return_date_from_string_value() {
        let value = Value::Str("1984-11-02".into());
        let casted = convert_to_date(SmartReference::Borrowed(&value));

        assert_eq!(
            casted.deref(),
            &Value::Date(NaiveDate::from_str("1984-11-02").unwrap()),
        );
    }

    #[test]
    fn convert_to_date_return_empty_from_string_value() {
        let value = Value::Str("npoe".into());
        let casted = convert_to_date(SmartReference::Borrowed(&value));

        assert_eq!(casted.deref(), &Value::Empty,);
    }

    #[test]
    fn convert_to_date_return_empty_from_bool_value() {
        let value = Value::Bool(true);
        let casted = convert_to_date(SmartReference::Borrowed(&value));

        assert_eq!(casted.deref(), &Value::Empty,);
    }

    #[test]
    fn convert_to_timestamp_return_empty() {
        let casted = convert_to_timestamp(Value::Empty.into());

        assert_eq!(casted.deref(), &Value::Empty,);
    }

    #[test]
    fn convert_to_timestamp_return_date_value() {
        let value = Value::Timestamp(NaiveDateTime::from_str("1984-11-02T11:11:11").unwrap());
        let casted = convert_to_timestamp(SmartReference::Borrowed(&value));

        assert_eq!(casted.deref(), &value,);
    }

    #[test]
    fn convert_to_date_return_timestamp_from_date_value() {
        let value = Value::Date(NaiveDate::from_str("1984-11-02").unwrap());
        let casted = convert_to_timestamp(SmartReference::Borrowed(&value));

        assert_eq!(
            casted.deref(),
            &Value::Timestamp(NaiveDateTime::from_str("1984-11-02T0:00:00").unwrap()),
        );
    }

    #[test]
    fn convert_to_timestamp_return_timestamp_from_string_value() {
        let value = Value::Str("1984-11-02 21:00:12".into());
        let casted = convert_to_timestamp(SmartReference::Borrowed(&value));

        assert_eq!(
            casted.deref(),
            &Value::Timestamp(NaiveDateTime::from_str("1984-11-02T21:00:12").unwrap()),
        );
    }

    #[test]
    fn convert_to_timestamp_return_empty_from_string_value() {
        let value = Value::Str("npoe".into());
        let casted = convert_to_timestamp(SmartReference::Borrowed(&value));

        assert_eq!(casted.deref(), &Value::Empty,);
    }

    #[test]
    fn convert_to_timestamp_return_empty_from_bool_value() {
        let value = Value::Bool(true);
        let casted = convert_to_timestamp(SmartReference::Borrowed(&value));

        assert_eq!(casted.deref(), &Value::Empty,);
    }
}
