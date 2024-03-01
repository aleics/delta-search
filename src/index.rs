use crate::{DataItemId, FieldValue, FilterOperation, FilterResult};
use indexmap::IndexSet;
use ordered_float::OrderedFloat;
use roaring::RoaringBitmap;
use std::collections::{BTreeMap, HashSet};
use std::ops::Bound;
use time::format_description::well_known::Iso8601;
use time::Date;

pub trait Indexable {
    fn id(&self) -> DataItemId;

    fn index_values(&self) -> Vec<IndexableValue>;
}

pub struct IndexableValue {
    pub(crate) name: String,
    pub(crate) value: FieldValue,
    pub(crate) descriptor: TypeDescriptor,
}

impl IndexableValue {
    pub fn string(name: String, value: String) -> Self {
        IndexableValue {
            name,
            value: FieldValue::string(value),
            descriptor: TypeDescriptor::String,
        }
    }

    pub fn numeric(name: String, value: f64) -> Self {
        IndexableValue {
            name,
            value: FieldValue::numeric(value),
            descriptor: TypeDescriptor::Numeric,
        }
    }

    pub fn enumerate(name: String, value: String, possibilities: HashSet<String>) -> Self {
        if !possibilities.contains(&value) {
            panic!(
                "Invalid enumerate index for \"{}\". Value \"{}\" not found in possibilities \"{:?}\"",
                name,
                value,
                possibilities
            );
        }

        IndexableValue {
            name,
            value: FieldValue::string(value),
            descriptor: TypeDescriptor::Enum(possibilities),
        }
    }

    pub fn date_iso(name: String, value: &str) -> Self {
        let date = Date::parse(value, &Iso8601::DEFAULT)
            .unwrap_or_else(|err| panic!("Date could not be parsed: {}", err));

        IndexableValue {
            name,
            value: FieldValue::date(date),
            descriptor: TypeDescriptor::Date,
        }
    }
}

pub(crate) enum TypeDescriptor {
    String,
    Numeric,
    Date,
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

#[derive(Clone)]
pub(crate) enum Index {
    String(StringIndex),
    Numeric(NumericIndex),
    Date(DateIndex),
    Enum(EnumIndex),
}

impl Index {
    pub(crate) fn from_type(value: &TypeDescriptor) -> Self {
        match value {
            TypeDescriptor::String => Index::String(StringIndex::new()),
            TypeDescriptor::Numeric => Index::Numeric(NumericIndex::new()),
            TypeDescriptor::Date => Index::Date(DateIndex::new()),
            TypeDescriptor::Enum(names) => {
                Index::Enum(EnumIndex::new(IndexSet::from_iter(names.clone())))
            }
        }
    }

    pub(crate) fn filter(&self, op: &FilterOperation) -> FilterResult {
        match self {
            Index::String(index) => index.filter(op),
            Index::Numeric(index) => index.filter(op),
            Index::Date(index) => index.filter(op),
            Index::Enum(index) => index.filter(op),
        }
    }

    pub(crate) fn sort(&self, items: &RoaringBitmap) -> Vec<u32> {
        match self {
            Index::String(index) => index.inner.sort(items),
            Index::Numeric(index) => index.inner.sort(items),
            Index::Date(index) => index.inner.sort(items),
            Index::Enum(index) => index.inner.sort(items),
        }
    }

    pub(crate) fn put(&mut self, value: FieldValue, position: u32) {
        match self {
            Index::String(index) => index.put(value, position),
            Index::Numeric(index) => index.put(value, position),
            Index::Date(index) => index.put(value, position),
            Index::Enum(index) => index.put(value, position),
        }
    }

    pub(crate) fn remove(&mut self, value: &FieldValue, position: u32) {
        match self {
            Index::String(index) => index.remove(value, position),
            Index::Numeric(index) => index.remove(value, position),
            Index::Date(index) => index.remove(value, position),
            Index::Enum(index) => index.remove(value, position),
        }
    }
}

#[derive(Default, Clone)]
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

#[derive(Default, Clone)]
pub(crate) struct NumericIndex {
    inner: SortableIndex<OrderedFloat<f64>>,
}

impl NumericIndex {
    fn new() -> Self {
        Self::default()
    }

    fn put(&mut self, value: FieldValue, position: u32) {
        let value = value
            .get_numeric()
            .expect("Numeric index only allows to insert numeric values.");

        self.inner.put(value, position);
    }

    fn remove(&mut self, value: &FieldValue, position: u32) {
        let value = value
            .as_numeric()
            .expect("Numeric index only allows to remove numeric values.");

        self.inner.remove(value, position);
    }
}

impl FilterableIndex for NumericIndex {
    fn equal(&self, value: &FieldValue) -> Option<RoaringBitmap> {
        let numeric_value = value
            .as_numeric()
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
                .as_numeric()
                .expect("Invalid \"between\" filter value. Expected numeric value.")
        });

        let second_bound = second.map(|value| {
            value
                .as_numeric()
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

#[derive(Default, Clone)]
pub(crate) struct DateIndex {
    inner: SortableIndex<i64>,
}

impl DateIndex {
    fn new() -> Self {
        Self::default()
    }

    fn put(&mut self, value: FieldValue, position: u32) {
        let value = value
            .get_date_epoch()
            .expect("Date index only allows to insert date values.");

        self.inner.put(value, position);
    }

    fn remove(&mut self, value: &FieldValue, position: u32) {
        let value = value
            .get_date_epoch()
            .expect("Date index only allows to remove date values.");

        self.inner.remove(&value, position);
    }
}

impl FilterableIndex for DateIndex {
    fn equal(&self, value: &FieldValue) -> Option<RoaringBitmap> {
        let date_value = value
            .get_date_epoch()
            .expect("Invalid value for \"equal\" filter. Expected date value.");

        self.inner.get(&date_value).cloned()
    }

    fn between(
        &self,
        first: Bound<&FieldValue>,
        second: Bound<&FieldValue>,
    ) -> Option<RoaringBitmap> {
        let first_bound = first.map(|value| {
            value
                .get_date_epoch()
                .expect("Invalid \"between\" filter value. Expected date value.")
        });

        let second_bound = second.map(|value| {
            value
                .get_date_epoch()
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

#[derive(Default, Clone)]
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

#[derive(Default, Debug, Clone)]
struct SortableIndex<T: Ord>(BTreeMap<T, RoaringBitmap>);

impl<T: Ord> SortableIndex<T> {
    /// Sort the provided `items` (ascendant, none last)
    fn sort(&self, items: &RoaringBitmap) -> Vec<u32> {
        let mut sorted = Vec::new();
        let mut not_found = RoaringBitmap::new();

        // Iterate over the tree of sorted values in the index
        for bitmap in self.0.values() {
            // Intersection between the value items and the input
            sorted.extend(items & bitmap);

            // Compute elements not present in the index by intersecting the difference of the
            // index values with the input. An item is not found if it's not present in any of
            // the indexed values.
            not_found &= items ^ bitmap;
        }

        sorted.extend(not_found);

        sorted
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
}
