use std::ops::Deref;

use bigdecimal::{BigDecimal, FromPrimitive};
use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use sqlparser::ast::DateTimeField;

use crate::{
    error::CvsSqlError, group_by::GroupRow, projections::Projection, util::SmartReference,
    value::Value,
};

struct TimeFieldExtractor {
    value: Box<dyn Projection>,
    field: Field,
    name: String,
}

impl Projection for TimeFieldExtractor {
    fn name(&self) -> &str {
        &self.name
    }
    fn get<'a>(&'a self, row: &'a GroupRow) -> SmartReference<'a, Value> {
        let value = self.value.get(row);
        match value.deref() {
            Value::Timestamp(ts) => extract_timestamp(&self.field, ts),
            Value::Date(dt) => extract_date(&self.field, dt),
            _ => Value::Empty.into(),
        }
    }
}

pub fn create_extract(
    field: &DateTimeField,
    value: Box<dyn Projection>,
) -> Result<Box<dyn Projection>, CvsSqlError> {
    let name = format!("EXTRACT({} FROM {})", field, value.name());
    let field = field.try_into()?;
    Ok(Box::new(TimeFieldExtractor { value, name, field }))
}
enum Field {
    Day,
    DayOfWeek,
    DayOfYear,
    Hour,
    Minute,
    Second,
    Epoch,
    Isodow,
    IsoWeek,
    Isoyear,
    Microsecond,
    Millisecond,
    Nanosecond,
}
impl TryFrom<&DateTimeField> for Field {
    type Error = CvsSqlError;
    fn try_from(value: &DateTimeField) -> Result<Self, Self::Error> {
        match value {
            DateTimeField::Day => Ok(Field::Day),
            DateTimeField::DayOfWeek => Ok(Field::DayOfWeek),
            DateTimeField::DayOfYear => Ok(Field::DayOfYear),
            DateTimeField::Hour => Ok(Field::Hour),
            DateTimeField::Minute => Ok(Field::Minute),
            DateTimeField::Second => Ok(Field::Second),
            DateTimeField::Dow => Ok(Field::DayOfWeek),
            DateTimeField::Doy => Ok(Field::DayOfYear),
            DateTimeField::Epoch => Ok(Field::Epoch),
            DateTimeField::Isodow => Ok(Field::Isodow),
            DateTimeField::IsoWeek => Ok(Field::IsoWeek),
            DateTimeField::Isoyear => Ok(Field::Isoyear),
            DateTimeField::Microsecond => Ok(Field::Microsecond),
            DateTimeField::Microseconds => Ok(Field::Microsecond),
            DateTimeField::Millisecond => Ok(Field::Millisecond),
            DateTimeField::Milliseconds => Ok(Field::Millisecond),
            DateTimeField::Nanosecond => Ok(Field::Nanosecond),
            DateTimeField::Nanoseconds => Ok(Field::Nanosecond),
            _ => Err(CvsSqlError::Unsupported(format!(
                "EXTRACT(... FROM {})",
                value
            ))),
        }
    }
}

fn from_u32<'a>(num: u32) -> SmartReference<'a, Value> {
    match BigDecimal::from_u32(num) {
        None => Value::Empty.into(),
        Some(num) => Value::Number(num).into(),
    }
}
fn from_i32<'a>(num: i32) -> SmartReference<'a, Value> {
    match BigDecimal::from_i32(num) {
        None => Value::Empty.into(),
        Some(num) => Value::Number(num).into(),
    }
}

fn from_epoc<'a>(ts: &NaiveDateTime) -> SmartReference<'a, Value> {
    match BigDecimal::from_i64(ts.and_utc().timestamp_micros()) {
        None => Value::Empty.into(),
        Some(num) => Value::Number(num / 1000000.0).into(),
    }
}
fn seconds<'a>(ts: &NaiveDateTime) -> SmartReference<'a, Value> {
    let Some(nanos) = BigDecimal::from_u32(ts.nanosecond()) else {
        return Value::Empty.into();
    };
    let Some(secs) = BigDecimal::from_u32(ts.second()) else {
        return Value::Empty.into();
    };
    Value::Number(secs + nanos / 1_000_000_000).into()
}
fn microseconds<'a>(ts: &NaiveDateTime) -> SmartReference<'a, Value> {
    let Some(nanos) = BigDecimal::from_u32(ts.nanosecond()) else {
        return Value::Empty.into();
    };
    let Some(secs) = BigDecimal::from_u32(ts.second()) else {
        return Value::Empty.into();
    };
    let ms: BigDecimal = secs * 1_000_000 + nanos / 1_000;
    Value::Number(ms.round(0)).into()
}
fn milliseconds<'a>(ts: &NaiveDateTime) -> SmartReference<'a, Value> {
    let Some(nanos) = BigDecimal::from_u32(ts.nanosecond()) else {
        return Value::Empty.into();
    };
    let Some(secs) = BigDecimal::from_u32(ts.second()) else {
        return Value::Empty.into();
    };
    Value::Number(secs * 1_000 + nanos / 1_000_000).into()
}

fn extract_timestamp<'a>(field: &'a Field, ts: &NaiveDateTime) -> SmartReference<'a, Value> {
    match field {
        Field::Day => from_u32(ts.day()),
        Field::DayOfWeek => from_u32(ts.weekday().num_days_from_sunday()),
        Field::DayOfYear => from_u32(ts.ordinal()),
        Field::Hour => from_u32(ts.hour()),
        Field::Minute => from_u32(ts.minute()),
        Field::Second => seconds(ts),
        Field::Epoch => from_epoc(ts),
        Field::Isodow => from_u32(ts.weekday().num_days_from_monday() + 1),
        Field::IsoWeek => from_u32(ts.iso_week().week()),
        Field::Isoyear => from_i32(ts.year()),
        Field::Microsecond => microseconds(ts),
        Field::Millisecond => milliseconds(ts),
        Field::Nanosecond => from_u32(ts.nanosecond()),
    }
}
fn extract_date<'a>(field: &'a Field, dt: &NaiveDate) -> SmartReference<'a, Value> {
    match field {
        Field::Day => from_u32(dt.day()),
        Field::DayOfWeek => from_u32(dt.weekday().num_days_from_sunday()),
        Field::DayOfYear => from_u32(dt.ordinal()),
        Field::Hour => from_u32(0),
        Field::Minute => from_u32(0),
        Field::Second => from_u32(0),
        Field::Epoch => from_epoc(&dt.and_time(NaiveTime::default())),
        Field::Isodow => from_u32(dt.weekday().num_days_from_monday() + 1),
        Field::IsoWeek => from_u32(dt.iso_week().week()),
        Field::Isoyear => from_i32(dt.year()),
        Field::Microsecond => from_u32(0),
        Field::Millisecond => from_u32(0),
        Field::Nanosecond => from_u32(0),
    }
}
