use std::collections::{BTreeMap, HashSet};
use std::fmt::Display;
use std::iter::FromIterator;
use std::ops::Bound;
use std::panic;

use crate::data::{date_to_timestamp, parse_date, timestamp_to_date, FieldValue};
use crate::query::{FilterName, FilterOperation, SortDirection};
use indexmap::IndexSet;
use ordered_float::OrderedFloat;
use roaring::{MultiOps, RoaringBitmap};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::format_description::well_known::Iso8601;

#[derive(Clone, Debug)]
pub enum TypeDescriptor {
    String,
    Numeric,
    Date,
    Bool,
    Enum(HashSet<String>),
}

trait FilterableIndex {
    fn filter(&self, op: &FilterOperation) -> Result<RoaringBitmap, FilterError> {
        match op {
            FilterOperation::Eq(value) => self.equal(value),
            FilterOperation::Between(first, second) => {
                self.between(Bound::Included(first), Bound::Included(second))
            }
            FilterOperation::GreaterThan(value) => {
                self.between(Bound::Excluded(value), Bound::Unbounded)
            }
            FilterOperation::GreaterOrEqual(value) => {
                self.between(Bound::Included(value), Bound::Unbounded)
            }
            FilterOperation::LessThan(value) => {
                self.between(Bound::Unbounded, Bound::Excluded(value))
            }
            FilterOperation::LessThanOrEqual(value) => {
                self.between(Bound::Unbounded, Bound::Included(value))
            }
        }
    }

    fn equal(&self, value: &FieldValue) -> Result<RoaringBitmap, FilterError>;

    fn between(
        &self,
        first: Bound<&FieldValue>,
        second: Bound<&FieldValue>,
    ) -> Result<RoaringBitmap, FilterError>;
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) enum Index {
    String(StringIndex),
    Numeric(NumericIndex),
    Date(DateIndex),
    Enum(EnumIndex),
    Bool(BoolIndex),
}

impl Index {
    pub(crate) fn create_descriptor(&self) -> TypeDescriptor {
        match self {
            Index::String(_) => TypeDescriptor::String,
            Index::Numeric(_) => TypeDescriptor::Numeric,
            Index::Date(_) => TypeDescriptor::Date,
            Index::Enum(index) => {
                let values = HashSet::from_iter(index.values.iter().cloned());
                TypeDescriptor::Enum(values)
            }
            Index::Bool(_) => TypeDescriptor::Bool,
        }
    }

    pub(crate) fn from_type(value: &TypeDescriptor) -> Self {
        match value {
            TypeDescriptor::String => Index::String(StringIndex::new()),
            TypeDescriptor::Numeric => Index::Numeric(NumericIndex::new()),
            TypeDescriptor::Date => Index::Date(DateIndex::new()),
            TypeDescriptor::Enum(names) => {
                Index::Enum(EnumIndex::new(IndexSet::from_iter(names.clone())))
            }
            TypeDescriptor::Bool => Index::Bool(BoolIndex::new()),
        }
    }

    pub(crate) fn filter(&self, op: &FilterOperation) -> Result<RoaringBitmap, FilterError> {
        match self {
            Index::String(index) => index.filter(op),
            Index::Numeric(index) => index.filter(op),
            Index::Date(index) => index.filter(op),
            Index::Enum(index) => index.filter(op),
            Index::Bool(index) => index.filter(op),
        }
    }

    pub(crate) fn sort(&self, items: &RoaringBitmap, direction: &SortDirection) -> Vec<u32> {
        match self {
            Index::String(index) => index.inner.sort(items, direction),
            Index::Numeric(index) => index.inner.sort(items, direction),
            Index::Date(index) => index.inner.sort(items, direction),
            Index::Enum(index) => index.inner.sort(items, direction),
            Index::Bool(index) => index.inner.sort(items, direction),
        }
    }

    pub(crate) fn get_value(&self, position: u32) -> Option<FieldValue> {
        match self {
            Index::String(index) => index.get_value(position),
            Index::Numeric(index) => index.get_value(position),
            Index::Date(index) => index.get_value(position),
            Index::Enum(index) => index.get_value(position),
            Index::Bool(index) => index.get_value(position),
        }
    }

    pub(crate) fn put(&mut self, value: FieldValue, position: u32) -> Result<(), IndexError> {
        match self {
            Index::String(index) => index.put(value, position),
            Index::Numeric(index) => index.put(value, position),
            Index::Date(index) => index.put(value, position),
            Index::Enum(index) => index.put(value, position),
            Index::Bool(index) => index.put(value, position),
        }
    }

    pub(crate) fn plus(&mut self, index: &Index) -> Result<(), IndexError> {
        match (self, index) {
            (Index::String(left), Index::String(right)) => left.plus(right),
            (Index::Numeric(left), Index::Numeric(right)) => left.plus(right),
            (Index::Date(left), Index::Date(right)) => left.plus(right),
            (Index::Enum(left), Index::Enum(right)) => left.plus(right),
            (Index::Bool(left), Index::Bool(right)) => left.plus(right),
            _ => {
                return Err(IndexError::UnsupportedOperation {
                    operation: "plus".to_string(),
                });
            }
        };

        Ok(())
    }

    pub(crate) fn minus(&mut self, index: &Index) -> Result<(), IndexError> {
        match (self, index) {
            (Index::String(left), Index::String(right)) => left.minus(right),
            (Index::Numeric(left), Index::Numeric(right)) => left.minus(right),
            (Index::Date(left), Index::Date(right)) => left.minus(right),
            (Index::Enum(left), Index::Enum(right)) => left.minus(right),
            (Index::Bool(left), Index::Bool(right)) => left.minus(right),
            _ => {
                return Err(IndexError::UnsupportedOperation {
                    operation: "minus".to_string(),
                });
            }
        };

        Ok(())
    }

    pub(crate) fn remove(&mut self, value: &FieldValue, position: u32) -> Result<(), IndexError> {
        match self {
            Index::String(index) => index.remove(value, position),
            Index::Numeric(index) => index.remove(value, position),
            Index::Date(index) => index.remove(value, position),
            Index::Enum(index) => index.remove(value, position),
            Index::Bool(index) => index.remove(value, position),
        }
    }

    pub(crate) fn remove_item(&mut self, position: u32) {
        match self {
            Index::String(index) => index.inner.remove_item(position),
            Index::Numeric(index) => index.inner.remove_item(position),
            Index::Date(index) => index.inner.remove_item(position),
            Index::Enum(index) => index.inner.remove_item(position),
            Index::Bool(index) => index.inner.remove_item(position),
        }
    }

    pub(crate) fn counts(&self, items: &RoaringBitmap) -> BTreeMap<String, u64> {
        match self {
            Index::String(index) => index.counts(items),
            Index::Numeric(index) => index.counts(items),
            Index::Date(_) => BTreeMap::new(), // TODO: create ranges
            Index::Enum(index) => index.counts(items),
            Index::Bool(index) => index.counts(items),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct StringIndex {
    inner: SortableIndex<String>,
}

impl StringIndex {
    fn new() -> Self {
        Self::default()
    }

    fn from_pairs<const N: usize>(arr: [(String, RoaringBitmap); N]) -> Self {
        StringIndex {
            inner: SortableIndex::from_pairs(arr),
        }
    }

    fn get_value(&self, position: u32) -> Option<FieldValue> {
        self.inner
            .get_value(position)
            .map(|value| FieldValue::str(value.as_str()))
    }

    fn put(&mut self, value: FieldValue, position: u32) -> Result<(), IndexError> {
        let Some(value) = value.get_string() else {
            return Err(IndexError::UnexpectedValue {
                expected_type: TypeName::String,
            });
        };

        self.inner.put(value, position);

        Ok(())
    }

    fn remove(&mut self, value: &FieldValue, position: u32) -> Result<(), IndexError> {
        let Some(value) = value.as_string() else {
            return Err(IndexError::UnexpectedValue {
                expected_type: TypeName::String,
            });
        };

        self.inner.remove(value, position);

        Ok(())
    }

    fn plus(&mut self, other: &StringIndex) {
        self.inner.plus(&other.inner)
    }

    fn minus(&mut self, other: &StringIndex) {
        self.inner.minus(&other.inner)
    }

    fn counts(&self, items: &RoaringBitmap) -> BTreeMap<String, u64> {
        self.inner
            .counts(items)
            .into_iter()
            .map(|(value, count)| (value.to_string(), count))
            .collect()
    }
}

impl FilterableIndex for StringIndex {
    fn equal(&self, value: &FieldValue) -> Result<RoaringBitmap, FilterError> {
        let Some(string_value) = value.as_string() else {
            return Err(FilterError::InvalidInput {
                filter: FilterName::Eq,
                type_name: TypeName::String,
            });
        };

        let hits = self
            .inner
            .get(string_value)
            .cloned()
            .unwrap_or_else(RoaringBitmap::new);

        Ok(hits)
    }

    fn between(
        &self,
        _: Bound<&FieldValue>,
        _: Bound<&FieldValue>,
    ) -> Result<RoaringBitmap, FilterError> {
        Err(FilterError::UnsupportedOperation {
            filter: FilterName::Between,
            type_name: TypeName::String,
        })
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct NumericIndex {
    inner: SortableIndex<OrderedFloat<f64>>,
}

impl NumericIndex {
    fn new() -> Self {
        Self::default()
    }

    fn from_pairs<const N: usize>(arr: [(OrderedFloat<f64>, RoaringBitmap); N]) -> Self {
        NumericIndex {
            inner: SortableIndex::from_pairs(arr),
        }
    }

    fn get_value(&self, position: u32) -> Option<FieldValue> {
        self.inner
            .get_value(position)
            .map(|value| FieldValue::Decimal(*value))
    }

    fn put(&mut self, value: FieldValue, position: u32) -> Result<(), IndexError> {
        let value = match value {
            FieldValue::Integer(value) => OrderedFloat(value as f64),
            FieldValue::Decimal(value) => value,
            _ => {
                return Err(IndexError::UnexpectedValue {
                    expected_type: TypeName::Numeric,
                })
            }
        };

        self.inner.put(value, position);

        Ok(())
    }

    fn remove(&mut self, value: &FieldValue, position: u32) -> Result<(), IndexError> {
        let Some(value) = value.as_decimal() else {
            return Err(IndexError::UnexpectedValue {
                expected_type: TypeName::Numeric,
            });
        };

        self.inner.remove(value, position);

        Ok(())
    }

    fn plus(&mut self, other: &NumericIndex) {
        self.inner.plus(&other.inner)
    }

    fn minus(&mut self, other: &NumericIndex) {
        self.inner.minus(&other.inner)
    }

    fn counts(&self, items: &RoaringBitmap) -> BTreeMap<String, u64> {
        self.inner
            .counts(items)
            .into_iter()
            .map(|(value, count)| (value.to_string(), count))
            .collect()
    }
}

impl FilterableIndex for NumericIndex {
    fn equal(&self, value: &FieldValue) -> Result<RoaringBitmap, FilterError> {
        let Some(numeric_value) = value.as_decimal() else {
            return Err(FilterError::InvalidInput {
                filter: FilterName::Eq,
                type_name: TypeName::Numeric,
            });
        };

        let hits = self
            .inner
            .get(numeric_value)
            .cloned()
            .unwrap_or_else(RoaringBitmap::new);

        Ok(hits)
    }

    fn between(
        &self,
        first: Bound<&FieldValue>,
        second: Bound<&FieldValue>,
    ) -> Result<RoaringBitmap, FilterError> {
        let first_bound = first.map(|value| {
            value
                .as_decimal()
                .expect("Invalid \"between\" filter value. Expected numeric value.")
        });

        let second_bound = second.map(|value| {
            value
                .as_decimal()
                .expect("Invalid \"between\" filter value. Expected numeric value.")
        });

        let mut matches = RoaringBitmap::new();
        for (_, bitmap) in self.inner.0.range((first_bound, second_bound)) {
            matches |= bitmap;
        }

        Ok(matches)
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct DateIndex {
    inner: SortableIndex<i64>,
}

impl DateIndex {
    fn new() -> Self {
        Self::default()
    }

    fn from_pairs<const N: usize>(arr: [(i64, RoaringBitmap); N]) -> Self {
        DateIndex {
            inner: SortableIndex::from_pairs(arr),
        }
    }

    fn parse_value(value: &FieldValue) -> Option<i64> {
        match value {
            FieldValue::String(string) => {
                let date = parse_date(string)
                    .unwrap_or_else(|err| panic!("Date could not be parsed: {}", err));

                Some(date_to_timestamp(date))
            }
            _ => None,
        }
    }

    fn get_value(&self, position: u32) -> Option<FieldValue> {
        let value = self.inner.get_value(position)?;
        let date = timestamp_to_date(*value)
            .format(&Iso8601::DEFAULT)
            .unwrap_or_else(|err| panic!("Date could not be formatted: {}", err));

        Some(FieldValue::String(date))
    }

    fn put(&mut self, value: FieldValue, position: u32) -> Result<(), IndexError> {
        let Some(value) = DateIndex::parse_value(&value) else {
            return Err(IndexError::UnexpectedValue {
                expected_type: TypeName::Date,
            });
        };

        self.inner.put(value, position);

        Ok(())
    }

    fn remove(&mut self, value: &FieldValue, position: u32) -> Result<(), IndexError> {
        let Some(value) = DateIndex::parse_value(value) else {
            return Err(IndexError::UnexpectedValue {
                expected_type: TypeName::Date,
            });
        };

        self.inner.remove(&value, position);

        Ok(())
    }

    fn plus(&mut self, other: &DateIndex) {
        self.inner.plus(&other.inner)
    }

    fn minus(&mut self, other: &DateIndex) {
        self.inner.minus(&other.inner)
    }
}

impl FilterableIndex for DateIndex {
    fn equal(&self, value: &FieldValue) -> Result<RoaringBitmap, FilterError> {
        let Some(date_value) = DateIndex::parse_value(value) else {
            return Err(FilterError::InvalidInput {
                filter: FilterName::Eq,
                type_name: TypeName::Date,
            });
        };

        let hits = self
            .inner
            .get(&date_value)
            .cloned()
            .unwrap_or_else(RoaringBitmap::new);

        Ok(hits)
    }

    fn between(
        &self,
        first: Bound<&FieldValue>,
        second: Bound<&FieldValue>,
    ) -> Result<RoaringBitmap, FilterError> {
        let first_bound = first.map(|value| {
            DateIndex::parse_value(value)
                .expect("Invalid \"between\" filter value. Expected date value.")
        });

        let second_bound = second.map(|value| {
            DateIndex::parse_value(value)
                .expect("Invalid \"between\" filter value. Expected date value.")
        });

        let mut matches = RoaringBitmap::new();
        for (_, bitmap) in self.inner.0.range((first_bound, second_bound)) {
            matches |= bitmap;
        }

        Ok(matches)
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct EnumIndex {
    values: IndexSet<String>,
    inner: SortableIndex<usize>,
}

impl EnumIndex {
    fn new(values: IndexSet<String>) -> Self {
        EnumIndex {
            values,
            inner: SortableIndex::default(),
        }
    }

    fn from_pairs<const N: usize>(
        values: IndexSet<String>,
        arr: [(usize, RoaringBitmap); N],
    ) -> Self {
        EnumIndex {
            values,
            inner: SortableIndex::from_pairs(arr),
        }
    }

    fn get_value(&self, position: u32) -> Option<FieldValue> {
        self.inner
            .get_value(position)
            .and_then(|value| self.values.get_index(*value))
            .map(|value| FieldValue::str(value.as_str()))
    }

    fn put(&mut self, value: FieldValue, position: u32) -> Result<(), IndexError> {
        let Some(value) = value.as_string() else {
            return Err(IndexError::UnexpectedValue {
                expected_type: TypeName::String,
            });
        };

        let Some(index) = self.values.get_index_of(value) else {
            return Err(IndexError::UnknownEnumValue {
                value: value.clone(),
            });
        };

        self.inner.put(index, position);

        Ok(())
    }

    fn remove(&mut self, value: &FieldValue, position: u32) -> Result<(), IndexError> {
        let Some(value) = value.as_string() else {
            return Err(IndexError::UnexpectedValue {
                expected_type: TypeName::String,
            });
        };

        let Some(index) = self.values.get_index_of(value) else {
            return Err(IndexError::UnknownEnumValue {
                value: value.clone(),
            });
        };

        self.inner.remove(&index, position);

        Ok(())
    }

    fn plus(&mut self, other: &EnumIndex) {
        self.inner.plus(&other.inner)
    }

    fn minus(&mut self, other: &EnumIndex) {
        self.inner.minus(&other.inner)
    }

    fn counts(&self, items: &RoaringBitmap) -> BTreeMap<String, u64> {
        self.inner
            .counts(items)
            .into_iter()
            .filter_map(|(value, count)| {
                self.values
                    .get_index(*value)
                    .map(|value| (value.to_string(), count))
            })
            .collect()
    }
}

impl FilterableIndex for EnumIndex {
    fn equal(&self, value: &FieldValue) -> Result<RoaringBitmap, FilterError> {
        let Some(string_value) = value.as_string() else {
            return Err(FilterError::InvalidInput {
                filter: FilterName::Eq,
                type_name: TypeName::Enum,
            });
        };

        let Some(index) = self.values.get_index_of(string_value) else {
            return Err(FilterError::UnknownEnumValue {
                filter: FilterName::Eq,
                value: string_value.to_string(),
            });
        };

        let hits = self
            .inner
            .get(&index)
            .cloned()
            .unwrap_or_else(RoaringBitmap::new);

        Ok(hits)
    }

    fn between(
        &self,
        _: Bound<&FieldValue>,
        _: Bound<&FieldValue>,
    ) -> Result<RoaringBitmap, FilterError> {
        panic!("Unsupported filter operation \"between\" for enum index")
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct BoolIndex {
    inner: SortableIndex<bool>,
}

impl BoolIndex {
    fn new() -> Self {
        BoolIndex {
            inner: SortableIndex::default(),
        }
    }

    fn from_pairs<const N: usize>(arr: [(bool, RoaringBitmap); N]) -> Self {
        BoolIndex {
            inner: SortableIndex::from_pairs(arr),
        }
    }

    fn get_value(&self, position: u32) -> Option<FieldValue> {
        self.inner
            .get_value(position)
            .map(|value| FieldValue::Bool(*value))
    }

    fn put(&mut self, value: FieldValue, position: u32) -> Result<(), IndexError> {
        let Some(value) = value.get_bool() else {
            return Err(IndexError::UnexpectedValue {
                expected_type: TypeName::Bool,
            });
        };

        self.inner.put(value, position);

        Ok(())
    }

    fn remove(&mut self, value: &FieldValue, position: u32) -> Result<(), IndexError> {
        let Some(value) = value.as_bool() else {
            return Err(IndexError::UnexpectedValue {
                expected_type: TypeName::Bool,
            });
        };

        self.inner.remove(value, position);

        Ok(())
    }

    fn plus(&mut self, other: &BoolIndex) {
        self.inner.plus(&other.inner)
    }

    fn minus(&mut self, other: &BoolIndex) {
        self.inner.minus(&other.inner)
    }

    fn counts(&self, items: &RoaringBitmap) -> BTreeMap<String, u64> {
        self.inner
            .counts(items)
            .into_iter()
            .map(|(value, count)| (value.to_string(), count))
            .collect()
    }
}

impl FilterableIndex for BoolIndex {
    fn equal(&self, value: &FieldValue) -> Result<RoaringBitmap, FilterError> {
        let Some(bool_value) = value.as_bool() else {
            return Err(FilterError::InvalidInput {
                filter: FilterName::Eq,
                type_name: TypeName::Bool,
            });
        };

        let hits = self
            .inner
            .get(bool_value)
            .cloned()
            .unwrap_or_else(RoaringBitmap::new);

        Ok(hits)
    }

    fn between(
        &self,
        _: Bound<&FieldValue>,
        _: Bound<&FieldValue>,
    ) -> Result<RoaringBitmap, FilterError> {
        panic!("Unsupported filter operation \"between\" for bool index")
    }
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
struct SortableIndex<T: Ord>(BTreeMap<T, RoaringBitmap>);

impl<T: Ord + Clone> SortableIndex<T> {
    fn from_pairs<const N: usize>(arr: [(T, RoaringBitmap); N]) -> Self {
        SortableIndex(BTreeMap::from(arr))
    }

    /// Sort the provided `items` by a certain direction
    fn sort(&self, items: &RoaringBitmap, direction: &SortDirection) -> Vec<u32> {
        match direction {
            SortDirection::ASC => SortableIndex::<T>::sort_by_iter(items, self.0.values()),
            SortDirection::DESC => SortableIndex::<T>::sort_by_iter(items, self.0.values().rev()),
        }
    }

    fn sort_by_iter<'a, I>(items: &RoaringBitmap, ordered_bitmaps: I) -> Vec<u32>
    where
        I: Iterator<Item = &'a RoaringBitmap>,
    {
        let mut sorted = Vec::new();
        let mut found = Vec::new();

        // Iterate over the tree of sorted values in the index
        for bitmap in ordered_bitmaps {
            // Intersection between the value items and the input
            let round = items & bitmap;

            sorted.extend(&round);
            found.push(round);
        }

        // Compute elements not present in the index by subtracting all the found elements
        // from the input. Use `union` for a faster union of the bitmaps instead of applying
        // the `BitOr` operation manually.
        sorted.extend(items - found.union());

        sorted
    }

    fn counts(&self, items: &RoaringBitmap) -> Vec<(&T, u64)> {
        let mut counts = Vec::new();

        for (value, bitmap) in &self.0 {
            counts.push((value, bitmap.intersection_len(items)))
        }

        counts
    }

    fn get_value(&self, position: u32) -> Option<&T> {
        for (value, bitmap) in &self.0 {
            if bitmap.contains(position) {
                return Some(value);
            }
        }
        None
    }

    fn get(&self, key: &T) -> Option<&RoaringBitmap> {
        self.0.get(key)
    }

    fn put(&mut self, key: T, position: u32) {
        let bitmap = self.0.entry(key).or_default();
        bitmap.insert(position);
    }

    fn remove(&mut self, key: &T, position: u32) {
        if let Some(bitmap) = self.0.get_mut(key) {
            bitmap.remove(position);
        }
    }

    fn plus(&mut self, other: &SortableIndex<T>) {
        for (key, right) in &other.0 {
            if let Some(left) = self.0.get_mut(key) {
                *left |= right;
            } else {
                self.0.insert(key.clone(), right.clone());
            }
        }
    }

    fn minus(&mut self, other: &SortableIndex<T>) {
        for (key, right) in &other.0 {
            if let Some(left) = self.0.get_mut(key) {
                *left -= right;
            }
        }
    }

    fn remove_item(&mut self, position: u32) {
        for bitmap in self.0.values_mut() {
            bitmap.remove(position);
        }
    }
}

#[derive(Clone, Debug)]
pub enum TypeName {
    String,
    Numeric,
    Date,
    Bool,
    Enum,
}

impl Display for TypeName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TypeName::String => write!(f, "string"),
            TypeName::Numeric => write!(f, "numeric"),
            TypeName::Date => write!(f, "date"),
            TypeName::Bool => write!(f, "bool"),
            TypeName::Enum => write!(f, "enum"),
        }
    }
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum IndexError {
    #[error("index operation \"{operation}\" is not supported")]
    UnsupportedOperation { operation: String },
    #[error("unexpected value for type {expected_type}")]
    UnexpectedValue { expected_type: TypeName },
    #[error("Value \"{value}\" is unknown for enum")]
    UnknownEnumValue { value: String },
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum FilterError {
    #[error("Invalid filter value for filter \"{filter}\". Expected {type_name} value")]
    InvalidInput {
        filter: FilterName,
        type_name: TypeName,
    },
    #[error("Unsupported operation for filter \"{filter}\". Expected {type_name} value")]
    UnsupportedOperation {
        filter: FilterName,
        type_name: TypeName,
    },
    #[error("Value \"{value}\" is unknown for enum in filter \"{filter}\"")]
    UnknownEnumValue { value: String, filter: FilterName },
}

#[cfg(test)]
mod tests {
    use roaring::RoaringBitmap;

    use crate::index::{Index, NumericIndex};

    #[test]
    fn index_plus() {
        // given
        let mut left = Index::Numeric(NumericIndex::from_pairs([
            (1.0.into(), RoaringBitmap::from([0])),
            (2.0.into(), RoaringBitmap::from([1])),
        ]));

        let right = Index::Numeric(NumericIndex::from_pairs([
            (1.0.into(), RoaringBitmap::from([1])),
            (3.0.into(), RoaringBitmap::from([2])),
        ]));

        // when
        left.plus(&right).unwrap();

        // then
        assert_eq!(
            left,
            Index::Numeric(NumericIndex::from_pairs([
                (1.0.into(), RoaringBitmap::from([0, 1])),
                (2.0.into(), RoaringBitmap::from([1])),
                (3.0.into(), RoaringBitmap::from([2]))
            ]))
        );
    }

    #[test]
    fn index_minus() {
        // given
        let mut left = Index::Numeric(NumericIndex::from_pairs([
            (1.0.into(), RoaringBitmap::from([0])),
            (2.0.into(), RoaringBitmap::from([1])),
        ]));

        let right = Index::Numeric(NumericIndex::from_pairs([(
            1.0.into(),
            RoaringBitmap::from([0, 1]),
        )]));

        // when
        left.minus(&right).unwrap();

        // then
        assert_eq!(
            left,
            Index::Numeric(NumericIndex::from_pairs([
                (1.0.into(), RoaringBitmap::from([])),
                (2.0.into(), RoaringBitmap::from([1])),
            ]))
        );
    }
}
