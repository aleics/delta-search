use num_traits::cast::FromPrimitive;
use ordered_float::OrderedFloat;
use serde::de::{Error, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};

pub type DataItemId = usize;

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
            .and_then(|field| field.as_integer())
            .and_then(|value| usize::try_from(*value).ok())
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
                let values_string = value
                    .iter()
                    .map(|value| format!("{}", value))
                    .intersperse(", ".into())
                    .collect::<String>();

                write!(f, "[{}]", values_string)
            }
        }
    }
}

