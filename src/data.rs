use std::collections::{BTreeMap, HashMap};
use std::fmt::{Display, Formatter};

use ordered_float::OrderedFloat;
use serde::de::{MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
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
    pub fields: BTreeMap<String, FieldValue>,
}

impl DataItem {
    /// Create a new item with a given identifier and a set of fields.
    pub fn new(id: DataItemId, fields: BTreeMap<String, FieldValue>) -> Self {
        DataItem { id, fields }
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
pub struct DataItemFieldsExternal {
    pub inner: BTreeMap<String, FieldValue>,
}

impl DataItemFieldsExternal {
    pub fn new(inner: BTreeMap<String, FieldValue>) -> Self {
        DataItemFieldsExternal { inner }
    }

    fn empty() -> Self {
        DataItemFieldsExternal {
            inner: BTreeMap::default(),
        }
    }
}

impl<'de> Deserialize<'de> for DataItemFieldsExternal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ExternalDataItemVisitor;

        impl<'de> Visitor<'de> for ExternalDataItemVisitor {
            type Value = DataItemFieldsExternal;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str(
                    "a key-value map with supported field values. At the time being, only string, numbers and dates are supported."
                )
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut item = DataItemFieldsExternal::empty();

                // Read the values as `InputFieldValue`, so that inner maps are flatten into a single key-value map
                // using a path structure for the flattened keys.
                while let Some((key, input_value)) =
                    map.next_entry::<String, Option<ExternalFieldValue>>()?
                {
                    let field_values = input_value
                        .map(|value| value.flatten(&key))
                        .unwrap_or_default();
                    item.inner.extend(field_values);
                }

                Ok(item)
            }
        }

        deserializer.deserialize_map(ExternalDataItemVisitor)
    }
}

impl Serialize for DataItemFieldsExternal {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.inner.len()))?;
        for (key, value) in &self.inner {
            map.serialize_entry(key, &as_external(value))?;
        }
        map.end()
    }
}

/// An intermediate structure used while deserializing input data so that
/// complex key-value maps are flattened into a single level.
#[derive(Deserialize, Serialize, Debug)]
#[serde(untagged)]
enum ExternalFieldValue {
    Bool(bool),
    Integer(u64),
    String(String),
    Decimal(f64),
    Map(HashMap<String, ExternalFieldValue>),
    Seq(Vec<ExternalFieldValue>),
}

impl ExternalFieldValue {
    fn flatten(self, key: &String) -> Vec<(String, FieldValue)> {
        let mut values = Vec::new();

        match self {
            ExternalFieldValue::Bool(value) => values.push((key.clone(), FieldValue::Bool(value))),
            ExternalFieldValue::Integer(value) => {
                values.push((key.clone(), FieldValue::Integer(value)))
            }
            ExternalFieldValue::String(value) => {
                values.push((key.clone(), FieldValue::String(value)))
            }
            ExternalFieldValue::Decimal(value) => {
                values.push((key.clone(), FieldValue::Decimal(OrderedFloat(value))))
            }
            ExternalFieldValue::Map(map) => {
                // Call recursively flatten for the inner maps and append a level to the keys
                for (inner_key, inner_value) in map {
                    let key = format!("{}.{}", key, inner_key);
                    values.append(&mut inner_value.flatten(&key));
                }
            }
            ExternalFieldValue::Seq(seq) => {
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

fn as_external(field: &FieldValue) -> ExternalFieldValue {
    match field {
        FieldValue::Bool(value) => ExternalFieldValue::Bool(*value),
        FieldValue::Integer(value) => ExternalFieldValue::Integer(*value),
        FieldValue::String(value) => ExternalFieldValue::String(value.clone()),
        FieldValue::Decimal(value) => ExternalFieldValue::Decimal(value.into_inner()),
        FieldValue::Array(value) => {
            ExternalFieldValue::Seq(value.iter().map(as_external).collect())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::data::{DataItemFieldsExternal, FieldValue};

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
        let data: DataItemFieldsExternal = serde_json::from_str(input).unwrap();

        // then
        assert_eq!(
            data,
            DataItemFieldsExternal::new(BTreeMap::from([
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

    #[test]
    fn serializes() {
        // given
        let input = DataItemFieldsExternal::new(BTreeMap::from([
            (
                "name".to_string(),
                FieldValue::String("Elephant".to_string()),
            ),
            ("type".to_string(), FieldValue::String("animal".to_string())),
            ("count_int".to_string(), FieldValue::int(415000)),
            ("count_float".to_string(), FieldValue::dec(415000.0)),
            ("carnivore".to_string(), FieldValue::Bool(false)),
            (
                "family.name".to_string(),
                FieldValue::String("Elephantidae".to_string()),
            ),
            (
                "family.characteristics.teeth_count".to_string(),
                FieldValue::int(26),
            ),
            (
                "regions.continent".to_string(),
                FieldValue::array([FieldValue::str("Asia"), FieldValue::str("Africa")]),
            ),
            (
                "regions.country".to_string(),
                FieldValue::array([FieldValue::str("Cambodia"), FieldValue::str("Tanzania")]),
            ),
            (
                "regions.cities.name".to_string(),
                FieldValue::array([
                    FieldValue::str("Phnom Penh"),
                    FieldValue::str("Siem Reap"),
                    FieldValue::str("Dodoma"),
                    FieldValue::str("Mwanza"),
                ]),
            ),
        ]));

        // when
        let output = serde_json::to_string(&input).unwrap();

        // then
        assert_eq!(
            normalize(&output),
            normalize(
                r#"{
                  "carnivore": false,
                  "count_float": 415000.0,
                  "count_int": 415000,
                  "family.characteristics.teeth_count": 26,
                  "family.name": "Elephantidae",
                  "name": "Elephant",
                  "regions.cities.name": ["PhnomPenh", "SiemReap", "Dodoma", "Mwanza"],
                  "regions.continent": ["Asia", "Africa"],
                  "regions.country": ["Cambodia", "Tanzania"],
                  "type": "animal"
                }"#
            )
        )
    }

    fn normalize(input: &str) -> String {
        let mut string = input.to_string();
        string.retain(|c| !c.is_whitespace());

        string
    }
}
