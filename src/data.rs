use num_traits::cast::FromPrimitive;
use ordered_float::OrderedFloat;
use serde::de::{Error, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use time::{Date, OffsetDateTime, Time};

pub(crate) fn date_to_timestamp(date: Date) -> i64 {
    OffsetDateTime::new_utc(date, Time::MIDNIGHT).unix_timestamp()
}

pub(crate) fn timestamp_to_date(timestamp: i64) -> Date {
    OffsetDateTime::from_unix_timestamp(timestamp)
        .expect("Could not parse timestamp as date time")
        .date()
}

pub type DataItemId = u64;

/// A data item is a generic representation of any element stored in the database.
/// Data items are identified using the `id` property and contain a set of fields.
#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataItem {
    pub id: DataItemId,
    pub fields: HashMap<String, FieldValue>,
}

impl DataItem {
    /// Create a new item with a given identifier and a set of fields.
    pub fn new(id: DataItemId, fields: HashMap<String, FieldValue>) -> Self {
        DataItem { id, fields }
    }

    /// Creates a new `DataItem` by reading the `input` and identifying the field
    /// used as identifier.
    pub fn from_input(id_field_name: &str, input: DataItemFieldsInput) -> Self {
        let id = input
            .inner
            .get(id_field_name)
            .and_then(|field| field.as_integer().copied())
            .unwrap_or_else(|| {
                panic!(
                    "Field \"{}\" not found in input data item or value type can't be used as ID.",
                    id_field_name
                )
            });

        DataItem {
            id,
            fields: input.inner,
        }
    }
}

/// A field value defines the available type of values that can be used inside
/// the stored data, so that, different indexes can be built using such values.
///
/// It's a plain wrapper from existing standard types.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FieldValue {
    Bool(bool),
    Integer(u64),
    String(String),
    Decimal(OrderedFloat<f64>),
    Array(Vec<FieldValue>),
}

impl FieldValue {
    pub fn bool(value: bool) -> FieldValue {
        FieldValue::Bool(value)
    }

    pub fn int(value: u64) -> FieldValue {
        FieldValue::Integer(value)
    }

    pub fn str(value: &str) -> FieldValue {
        FieldValue::String(value.to_string())
    }

    pub fn dec(value: f64) -> FieldValue {
        FieldValue::Decimal(OrderedFloat(value))
    }

    pub fn array<const N: usize>(values: [FieldValue; N]) -> FieldValue {
        FieldValue::Array(values.to_vec())
    }

    pub(crate) fn as_bool(&self) -> Option<&bool> {
        if let FieldValue::Bool(value) = self {
            Some(value)
        } else {
            None
        }
    }

    pub(crate) fn get_bool(self) -> Option<bool> {
        if let FieldValue::Bool(value) = self {
            Some(value)
        } else {
            None
        }
    }

    pub(crate) fn as_integer(&self) -> Option<&u64> {
        if let FieldValue::Integer(value) = self {
            Some(value)
        } else {
            None
        }
    }

    pub(crate) fn as_decimal(&self) -> Option<&OrderedFloat<f64>> {
        if let FieldValue::Decimal(value) = self {
            Some(value)
        } else {
            None
        }
    }

    pub(crate) fn get_decimal(self) -> Option<OrderedFloat<f64>> {
        if let FieldValue::Decimal(value) = self {
            Some(value)
        } else {
            None
        }
    }

    pub(crate) fn as_string(&self) -> Option<&String> {
        if let FieldValue::String(value) = self {
            Some(value)
        } else {
            None
        }
    }

    pub(crate) fn get_string(self) -> Option<String> {
        if let FieldValue::String(value) = self {
            Some(value)
        } else {
            None
        }
    }
}

impl Display for FieldValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FieldValue::Bool(value) => write!(f, "{}", value),
            FieldValue::Integer(value) => write!(f, "{}", value),
            FieldValue::String(value) => write!(f, "{}", value),
            FieldValue::Decimal(value) => write!(f, "{}", value.0),
            FieldValue::Array(value) => {
                let values = value
                    .iter()
                    .map(|value| format!("{}", value))
                    .collect::<Vec<String>>();

                write!(f, "[{}]", values.join(", "))
            }
        }
    }
}

/// The input data item is used as a merely intermediate step when deserializing
/// input data items. A custom deserializer is used to process the data in a
/// way that it's compatible with `DataItem` and the inner's storage logic.
#[derive(Default, Debug, PartialEq)]
pub struct DataItemFieldsInput {
    pub inner: HashMap<String, FieldValue>,
}

impl DataItemFieldsInput {
    fn new(inner: HashMap<String, FieldValue>) -> Self {
        DataItemFieldsInput { inner }
    }

    fn with_capacity(size: usize) -> Self {
        DataItemFieldsInput {
            inner: HashMap::with_capacity(size),
        }
    }
}

struct InputDataItemVisitor;

impl<'de> Visitor<'de> for InputDataItemVisitor {
    type Value = DataItemFieldsInput;

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        formatter.write_str(
            "a key-value map with supported field values. At the time being, only string, numbers and dates are supported."
        )
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut item = DataItemFieldsInput::with_capacity(map.size_hint().unwrap_or(0));

        // Read the values as `InputFieldValue`, so that inner maps are flatten into a single key-value map
        // using a path structure for the flattened keys.
        while let Some((key, input_value)) = map.next_entry::<String, Option<InputFieldValue>>()? {
            let field_values = input_value
                .map(|value| value.flatten(&key))
                .unwrap_or_default();
            item.inner.extend(field_values);
        }

        Ok(item)
    }
}

impl<'de> Deserialize<'de> for DataItemFieldsInput {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(InputDataItemVisitor)
    }
}

/// An intermediate structure used while deserializing input data so that
/// complex key-value maps are flattened into a single level.
enum InputFieldValue {
    Literal(FieldValue),
    Map(HashMap<String, InputFieldValue>),
    Seq(Vec<InputFieldValue>),
}

impl InputFieldValue {
    fn flatten(self, key: &String) -> Vec<(String, FieldValue)> {
        let mut values = Vec::new();

        match self {
            InputFieldValue::Literal(value) => values.push((key.clone(), value)),
            InputFieldValue::Map(map) => {
                // Call recursively flatten for the inner maps and append a level to the keys
                for (inner_key, inner_value) in map {
                    let key = format!("{}.{}", key, inner_key);
                    values.append(&mut inner_value.flatten(&key));
                }
            }
            InputFieldValue::Seq(seq) => {
                // Call recursively flatten for each element in the sequence and append a level
                // to the keys
                let mut inner_values: HashMap<String, Vec<FieldValue>> =
                    HashMap::with_capacity(seq.len());

                for value in seq {
                    for (inner_key, inner_value) in value.flatten(key) {
                        let existing = inner_values.entry(inner_key).or_default();

                        // In case the inner value is an array already, unwrap its values
                        if let FieldValue::Array(array_values) = inner_value {
                            existing.extend(array_values);
                        } else {
                            existing.push(inner_value);
                        }
                    }
                }

                for (key, array_values) in inner_values {
                    values.push((key, FieldValue::Array(array_values)));
                }
            }
        }

        values
    }
}

struct InputFieldValueVisitor;

impl<'de> Visitor<'de> for InputFieldValueVisitor {
    type Value = InputFieldValue;

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        formatter.write_str(
            "a supported field value. At the time being, only string, numbers and dates are supported."
        )
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(InputFieldValue::Literal(FieldValue::Bool(value)))
    }

    fn visit_i8<E>(self, v: i8) -> Result<Self::Value, E>
    where
        E: Error,
    {
        let number = f64::from(v);
        Ok(InputFieldValue::Literal(FieldValue::Decimal(OrderedFloat(
            number,
        ))))
    }

    fn visit_i16<E>(self, v: i16) -> Result<Self::Value, E>
    where
        E: Error,
    {
        let number = f64::from(v);
        Ok(InputFieldValue::Literal(FieldValue::Decimal(OrderedFloat(
            number,
        ))))
    }

    fn visit_i32<E>(self, v: i32) -> Result<Self::Value, E>
    where
        E: Error,
    {
        let number = f64::from(v);
        Ok(InputFieldValue::Literal(FieldValue::Decimal(OrderedFloat(
            number,
        ))))
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: Error,
    {
        let number = f64::from_i64(v).unwrap_or(0.0);
        Ok(InputFieldValue::Literal(FieldValue::Decimal(OrderedFloat(
            number,
        ))))
    }

    fn visit_i128<E>(self, v: i128) -> Result<Self::Value, E>
    where
        E: Error,
    {
        let number = f64::from_i128(v).unwrap_or(0.0);
        Ok(InputFieldValue::Literal(FieldValue::Decimal(OrderedFloat(
            number,
        ))))
    }

    fn visit_u8<E>(self, v: u8) -> Result<Self::Value, E>
    where
        E: Error,
    {
        let number = u64::from(v);
        Ok(InputFieldValue::Literal(FieldValue::int(number)))
    }

    fn visit_u16<E>(self, v: u16) -> Result<Self::Value, E>
    where
        E: Error,
    {
        let number = u64::from(v);
        Ok(InputFieldValue::Literal(FieldValue::int(number)))
    }

    fn visit_u32<E>(self, v: u32) -> Result<Self::Value, E>
    where
        E: Error,
    {
        let number = u64::from(v);
        Ok(InputFieldValue::Literal(FieldValue::int(number)))
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(InputFieldValue::Literal(FieldValue::int(v)))
    }

    fn visit_f32<E>(self, v: f32) -> Result<Self::Value, E>
    where
        E: Error,
    {
        let number = f64::from(v);
        Ok(InputFieldValue::Literal(FieldValue::dec(number)))
    }

    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(InputFieldValue::Literal(FieldValue::dec(v)))
    }

    fn visit_char<E>(self, v: char) -> Result<Self::Value, E>
    where
        E: Error,
    {
        let string = v.to_string();
        Ok(InputFieldValue::Literal(FieldValue::String(string)))
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(InputFieldValue::Literal(FieldValue::str(v)))
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(InputFieldValue::Literal(FieldValue::String(v)))
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::with_capacity(seq.size_hint().unwrap_or(0));

        while let Some(element) = seq.next_element().unwrap() {
            values.push(element);
        }

        Ok(InputFieldValue::Seq(values))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut values = HashMap::with_capacity(map.size_hint().unwrap_or(0));
        while let Some((key, value)) = map.next_entry()? {
            values.insert(key, value);
        }
        Ok(InputFieldValue::Map(values))
    }
}

impl<'de> Deserialize<'de> for InputFieldValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(InputFieldValueVisitor)
    }
}

#[cfg(test)]
mod tests {
    use crate::data::{DataItemFieldsInput, FieldValue};
    use std::collections::HashMap;

    #[test]
    fn deserializes() {
        // given
        let input = r#"{
            "name": "Elephant",
            "type": "animal",
            "count_int": 415000,
            "count_float": 415000.0,
            "carnivore": false,
            "family": {
                "name": "Elephantidae",
                "characteristics": {
                    "teeth_count": 26
                }
            },
            "regions": [
                {
                    "continent": "Asia",
                    "country": "Cambodia",
                    "cities": [
                        {
                            "name": "Phnom Penh"
                        },
                        {
                            "name": "Siem Reap"
                        }
                    ]
                },
                {
                    "continent": "Africa",
                    "country": "Tanzania",
                    "cities": [
                        {
                            "name": "Dodoma"
                        },
                        {
                            "name": "Mwanza"
                        }
                    ]
                }
            ]
        }"#;

        // when
        let data: DataItemFieldsInput = serde_json::from_str(input).unwrap();

        // then
        assert_eq!(
            data,
            DataItemFieldsInput::new(HashMap::from([
                (
                    "name".to_string(),
                    FieldValue::String("Elephant".to_string())
                ),
                ("type".to_string(), FieldValue::String("animal".to_string())),
                ("count_int".to_string(), FieldValue::int(415000)),
                ("count_float".to_string(), FieldValue::dec(415000.0)),
                ("carnivore".to_string(), FieldValue::Bool(false)),
                (
                    "family.name".to_string(),
                    FieldValue::String("Elephantidae".to_string())
                ),
                (
                    "family.characteristics.teeth_count".to_string(),
                    FieldValue::int(26)
                ),
                (
                    "regions.continent".to_string(),
                    FieldValue::array([FieldValue::str("Asia"), FieldValue::str("Africa")])
                ),
                (
                    "regions.country".to_string(),
                    FieldValue::array([FieldValue::str("Cambodia"), FieldValue::str("Tanzania")])
                ),
                (
                    "regions.cities.name".to_string(),
                    FieldValue::array([
                        FieldValue::str("Phnom Penh"),
                        FieldValue::str("Siem Reap"),
                        FieldValue::str("Dodoma"),
                        FieldValue::str("Mwanza")
                    ])
                )
            ]))
        )
    }
}
