use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Bound;
use std::panic;

use indexmap::IndexSet;
use ordered_float::OrderedFloat;
use roaring::{MultiOps, RoaringBitmap};
use serde::{Deserialize, Serialize};
use time::format_description::well_known::Iso8601;
use time::Date;

use crate::data::{date_to_timestamp, FieldValue};
use crate::query::{FilterOperation, FilterResult, SortDirection};

#[derive(Clone)]
pub enum TypeDescriptor {
    String,
    Numeric,
    Date,
    Bool,
    Enum(HashSet<String>),
}

trait FilterableIndex {
    fn filter(&self, op: &FilterOperation) -> FilterResult {
        let hits = match op {
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
        };

        hits.map(FilterResult::new)
            .unwrap_or_else(FilterResult::empty)
    }

    fn equal(&self, value: &FieldValue) -> Option<RoaringBitmap>;

    fn between(
        &self,
        first: Bound<&FieldValue>,
        second: Bound<&FieldValue>,
    ) -> Option<RoaringBitmap>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

    pub(crate) fn filter(&self, op: &FilterOperation) -> FilterResult {
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

    pub(crate) fn put(&mut self, value: FieldValue, position: u32) {
        match self {
            Index::String(index) => index.put(value, position),
            Index::Numeric(index) => index.put(value, position),
            Index::Date(index) => index.put(value, position),
            Index::Enum(index) => index.put(value, position),
            Index::Bool(index) => index.put(value, position),
        }
    }

    pub(crate) fn remove(&mut self, value: &FieldValue, position: u32) {
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

    pub(crate) fn counts(&self, items: &RoaringBitmap) -> HashMap<String, u64> {
        match self {
            Index::String(index) => index.counts(items),
            Index::Numeric(index) => index.counts(items),
            Index::Date(_) => HashMap::new(), // TODO: create ranges
            Index::Enum(index) => index.counts(items),
            Index::Bool(index) => index.counts(items),
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(crate) struct StringIndex {
    inner: SortableIndex<String>,
}

impl StringIndex {
    fn new() -> Self {
        Self::default()
    }

    fn put(&mut self, value: FieldValue, position: u32) {
        let value = value
            .get_string()
            .expect("String index only allows to insert string values.");

        self.inner.put(value, position);
    }

    fn remove(&mut self, value: &FieldValue, position: u32) {
        let value = value
            .as_string()
            .expect("String index only allows to remove string values.");

        self.inner.remove(value, position);
    }

    fn counts(&self, items: &RoaringBitmap) -> HashMap<String, u64> {
        self.inner
            .counts(items)
            .into_iter()
            .map(|(value, count)| (value.to_string(), count))
            .collect()
    }
}

impl FilterableIndex for StringIndex {
    fn equal(&self, value: &FieldValue) -> Option<RoaringBitmap> {
        let string_value = value
            .as_string()
            .expect("Invalid value for \"equal\" filter. Expected string value.");

        self.inner.get(string_value).cloned()
    }

    fn between(&self, _: Bound<&FieldValue>, _: Bound<&FieldValue>) -> Option<RoaringBitmap> {
        panic!("Unsupported filter operation \"between\" for string index")
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(crate) struct NumericIndex {
    inner: SortableIndex<OrderedFloat<f64>>,
}

impl NumericIndex {
    fn new() -> Self {
        Self::default()
    }

    fn put(&mut self, value: FieldValue, position: u32) {
        let value = match value {
            FieldValue::Integer(value) => OrderedFloat(value as f64),
            FieldValue::Decimal(value) => value,
            _ => panic!("Numeric index only allows to insert numeric values."),
        };

        self.inner.put(value, position);
    }

    fn remove(&mut self, value: &FieldValue, position: u32) {
        let value = value
            .as_decimal()
            .expect("Numeric index only allows to remove numeric values.");

        self.inner.remove(value, position);
    }

    fn counts(&self, items: &RoaringBitmap) -> HashMap<String, u64> {
        self.inner
            .counts(items)
            .into_iter()
            .map(|(value, count)| (value.to_string(), count))
            .collect()
    }
}

impl FilterableIndex for NumericIndex {
    fn equal(&self, value: &FieldValue) -> Option<RoaringBitmap> {
        let numeric_value = value
            .as_decimal()
            .expect("Invalid value for \"equal\" filter. Expected numeric value.");

        self.inner.get(numeric_value).cloned()
    }

    fn between(
        &self,
        first: Bound<&FieldValue>,
        second: Bound<&FieldValue>,
    ) -> Option<RoaringBitmap> {
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

        if matches.is_empty() {
            None
        } else {
            Some(matches)
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(crate) struct DateIndex {
    inner: SortableIndex<i64>,
}

impl DateIndex {
    fn new() -> Self {
        Self::default()
    }

    fn parse_value(value: &FieldValue) -> Option<i64> {
        match value {
            FieldValue::String(string) => {
                let date = Date::parse(string, &Iso8601::DEFAULT)
                    .unwrap_or_else(|err| panic!("Date could not be parsed: {}", err));

                Some(date_to_timestamp(date))
            }
            _ => None,
        }
    }

    fn put(&mut self, value: FieldValue, position: u32) {
        let value =
            DateIndex::parse_value(&value).expect("Date index only allows to insert date values.");

        self.inner.put(value, position);
    }

    fn remove(&mut self, value: &FieldValue, position: u32) {
        let value =
            DateIndex::parse_value(value).expect("Date index only allows to remove date values.");

        self.inner.remove(&value, position);
    }
}

impl FilterableIndex for DateIndex {
    fn equal(&self, value: &FieldValue) -> Option<RoaringBitmap> {
        let date_value = DateIndex::parse_value(value)
            .expect("Invalid value for \"equal\" filter. Expected date value.");

        self.inner.get(&date_value).cloned()
    }

    fn between(
        &self,
        first: Bound<&FieldValue>,
        second: Bound<&FieldValue>,
    ) -> Option<RoaringBitmap> {
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

        if matches.is_empty() {
            None
        } else {
            Some(matches)
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
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

    fn put(&mut self, value: FieldValue, position: u32) {
        let value = value
            .as_string()
            .expect("Enum index only allows to insert string values.");

        let index = self
            .values
            .get_index_of(value)
            .expect("Enum index does not know value to be inserted.");

        self.inner.put(index, position);
    }

    fn remove(&mut self, value: &FieldValue, position: u32) {
        let value = value
            .as_string()
            .expect("Enum index only allows to remove string values.");

        let index = self
            .values
            .get_index_of(value)
            .expect("Enum index does not know value to be removed.");

        self.inner.remove(&index, position);
    }

    fn counts(&self, items: &RoaringBitmap) -> HashMap<String, u64> {
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
    fn equal(&self, value: &FieldValue) -> Option<RoaringBitmap> {
        let string_value = value
            .as_string()
            .expect("Enum index only supports string values for \"equal\" filter.");

        let index = self.values.get_index_of(string_value)?;
        self.inner.get(&index).cloned()
    }

    fn between(&self, _: Bound<&FieldValue>, _: Bound<&FieldValue>) -> Option<RoaringBitmap> {
        panic!("Unsupported filter operation \"between\" for enum index")
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(crate) struct BoolIndex {
    inner: SortableIndex<bool>,
}

impl BoolIndex {
    fn new() -> Self {
        BoolIndex {
            inner: SortableIndex::default(),
        }
    }

    fn put(&mut self, value: FieldValue, position: u32) {
        let value = value
            .get_bool()
            .expect("Bool index only allows to insert bool values.");

        self.inner.put(value, position);
    }

    fn remove(&mut self, value: &FieldValue, position: u32) {
        let value = value
            .as_bool()
            .expect("Bool index only allows to remove bool values.");

        self.inner.remove(value, position);
    }

    fn counts(&self, items: &RoaringBitmap) -> HashMap<String, u64> {
        self.inner
            .counts(items)
            .into_iter()
            .map(|(value, count)| (value.to_string(), count))
            .collect()
    }
}

impl FilterableIndex for BoolIndex {
    fn equal(&self, value: &FieldValue) -> Option<RoaringBitmap> {
        let bool_value = value
            .as_bool()
            .expect("Invalid value for \"equal\" filter. Expected bool value.");

        self.inner.get(bool_value).cloned()
    }

    fn between(&self, _: Bound<&FieldValue>, _: Bound<&FieldValue>) -> Option<RoaringBitmap> {
        panic!("Unsupported filter operation \"between\" for bool index")
    }
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
struct SortableIndex<T: Ord>(BTreeMap<T, RoaringBitmap>);

impl<T: Ord> SortableIndex<T> {
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
            let count = bitmap.intersection_len(items);
            if count > 0 {
                counts.push((value, count));
            }
        }

        counts
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

    fn remove_item(&mut self, position: u32) {
        for bitmap in self.0.values_mut() {
            bitmap.remove(position);
        }
    }
}
