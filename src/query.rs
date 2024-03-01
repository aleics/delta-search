use crate::index::{Index, Indexable};
use crate::{DataItemId, EntityStorage, FieldValue};
use roaring::RoaringBitmap;
use std::collections::HashMap;

struct QueryIndices<'a> {
    stored: &'a HashMap<String, Index>,
    deltas: HashMap<String, Index>,
}

impl<'a> QueryIndices<'a> {
    fn new(stored: &'a HashMap<String, Index>) -> Self {
        QueryIndices {
            stored,
            deltas: HashMap::new(),
        }
    }

    fn attach_deltas<T>(mut self, deltas: &[BoxedDelta<T>], storage: &EntityStorage<T>) -> Self
    where
        T: Indexable,
    {
        for delta in deltas {
            let change = delta.change();

            if let Some(current) = self.stored.get(&change.scope.field_name) {
                if let Some(position) = storage.get_position_by_id(&change.scope.id) {
                    let mut dynamic = current.clone();

                    if let Some(before) = change.before.as_ref() {
                        dynamic.remove(before, *position);
                    }

                    if let Some(after) = change.after {
                        dynamic.put(after, *position);
                    }

                    self.deltas.insert(change.scope.field_name, dynamic);
                }
            }
        }
        self
    }

    fn get(&self, name: &String) -> Option<&Index> {
        self.deltas.get(name).or_else(|| self.stored.get(name))
    }

    fn execute_filter(&self, filter: &CompositeFilter) -> FilterResult {
        match filter {
            CompositeFilter::And(filters) => {
                let result: Option<FilterResult> = filters.iter().fold(None, |acc, filter| {
                    acc.map(|current| current.and(self.execute_filter(filter)))
                });

                result.unwrap_or_else(FilterResult::empty)
            }
            CompositeFilter::Or(filters) => {
                let result: Option<FilterResult> = filters.iter().fold(None, |acc, filter| {
                    acc.map(|current| current.or(self.execute_filter(filter)))
                });

                result.unwrap_or_else(FilterResult::empty)
            }
            CompositeFilter::Single(filter) => {
                let index = self.get(&filter.name).unwrap_or_else(|| {
                    panic!("Filter with name {} has no index assigned", &filter.name)
                });

                index.filter(&filter.operation)
            }
        }
    }

    fn execute_sort(&self, items: &RoaringBitmap, sort: &Sort) -> Vec<u32> {
        let index = self
            .get(&sort.by)
            .expect("Sort by criteria does not have an index.");

        index.sort(items)
    }
}

pub struct QueryExecution<T> {
    filter: CompositeFilter,
    deltas: Vec<BoxedDelta<T>>,
    sort: Option<Sort>,
    pagination: Option<Pagination>,
}

impl<T: Indexable + Clone> QueryExecution<T> {
    pub fn new(filter: CompositeFilter) -> Self {
        QueryExecution {
            deltas: Vec::new(),
            filter,
            pagination: None,
            sort: None,
        }
    }

    pub fn with_deltas<D>(mut self, deltas: Vec<D>) -> Self
    where
        D: Delta<Value = T> + 'static,
    {
        for delta in deltas {
            self.deltas.push(Box::new(delta));
        }
        self
    }

    pub fn with_sort(mut self, sort: Sort) -> Self {
        self.sort = Some(sort);
        self
    }

    pub fn with_pagination(mut self, pagination: Pagination) -> Self {
        self.pagination = Some(pagination);
        self
    }

    pub fn run(self, storage: &EntityStorage<T>) -> Vec<T> {
        let indices = QueryIndices::new(&storage.indices).attach_deltas(&self.deltas, storage);

        let filter_result = indices.execute_filter(&self.filter);
        let item_ids = self.sort(filter_result, &indices, storage);

        self.read_data(&item_ids, storage)
    }

    fn sort(
        &self,
        filter_result: FilterResult,
        indices: &QueryIndices,
        storage: &EntityStorage<T>,
    ) -> Vec<DataItemId> {
        if let Some(sort) = &self.sort {
            return indices
                .execute_sort(&filter_result.hits, sort)
                .iter()
                .flat_map(|position| storage.get_id_by_position(position))
                .copied()
                .collect();
        }

        filter_result
            .hits
            .iter()
            .flat_map(|position| storage.get_id_by_position(&position))
            .copied()
            .collect()
    }

    fn read_data(&self, ids: &[DataItemId], storage: &EntityStorage<T>) -> Vec<T> {
        let mut data = Vec::new();

        let deltas_by_id: HashMap<DataItemId, Vec<&BoxedDelta<T>>> =
            self.deltas.iter().fold(HashMap::new(), |mut acc, delta| {
                let key = delta.change().scope.id;
                acc.entry(key).or_default().push(delta);
                acc
            });

        let pagination = self.pagination.unwrap_or(Pagination::new(0, ids.len()));

        for id in ids.iter().skip(pagination.start).take(pagination.size) {
            if let Some(mut item) = storage.data.get(id).cloned() {
                if let Some(deltas) = deltas_by_id.get(id) {
                    for delta in deltas {
                        delta.apply_data(&mut item);
                    }
                }

                data.push(item);
            }
        }

        data
    }
}

#[derive(Debug)]
pub enum CompositeFilter {
    And(Vec<CompositeFilter>),
    Or(Vec<CompositeFilter>),
    Single(Filter),
}

impl CompositeFilter {
    pub fn eq(name: &str, value: FieldValue) -> Self {
        CompositeFilter::Single(Filter {
            name: name.to_string(),
            operation: FilterOperation::Eq(value),
        })
    }

    pub fn between(name: &str, first: FieldValue, second: FieldValue) -> Self {
        CompositeFilter::Single(Filter {
            name: name.to_string(),
            operation: FilterOperation::Between(first, second),
        })
    }

    pub fn gt(name: &str, value: FieldValue) -> Self {
        CompositeFilter::Single(Filter {
            name: name.to_string(),
            operation: FilterOperation::GreaterThan(value),
        })
    }

    pub fn ge(name: &str, value: FieldValue) -> Self {
        CompositeFilter::Single(Filter {
            name: name.to_string(),
            operation: FilterOperation::GreaterOrEqual(value),
        })
    }

    pub fn lt(name: &str, value: FieldValue) -> Self {
        CompositeFilter::Single(Filter {
            name: name.to_string(),
            operation: FilterOperation::LessThan(value),
        })
    }

    pub fn le(name: &str, value: FieldValue) -> Self {
        CompositeFilter::Single(Filter {
            name: name.to_string(),
            operation: FilterOperation::LessThanOrEqual(value),
        })
    }

    pub fn or(filters: Vec<CompositeFilter>) -> Self {
        CompositeFilter::Or(filters)
    }

    pub fn and(filters: Vec<CompositeFilter>) -> Self {
        CompositeFilter::And(filters)
    }
}

#[derive(Clone)]
pub struct FilterResult {
    hits: RoaringBitmap,
}

impl FilterResult {
    pub(crate) fn new(hits: RoaringBitmap) -> Self {
        FilterResult { hits }
    }

    pub(crate) fn empty() -> Self {
        FilterResult {
            hits: RoaringBitmap::new(),
        }
    }

    fn and(self, another: FilterResult) -> FilterResult {
        FilterResult {
            hits: self.hits & another.hits,
        }
    }

    fn or(self, another: FilterResult) -> FilterResult {
        FilterResult {
            hits: self.hits | another.hits,
        }
    }
}

#[derive(Copy, Clone)]
pub struct Pagination {
    start: usize,
    size: usize,
}

impl Pagination {
    pub fn new(start: usize, size: usize) -> Self {
        Pagination { start, size }
    }
}

pub struct Sort {
    by: String,
}

impl Sort {
    pub fn new(by: String) -> Self {
        Sort { by }
    }
}

#[derive(Debug)]
pub struct Filter {
    name: String,
    operation: FilterOperation,
}

#[derive(Debug)]
pub enum FilterOperation {
    Eq(FieldValue),
    Between(FieldValue, FieldValue),
    GreaterThan(FieldValue),
    GreaterOrEqual(FieldValue),
    LessThan(FieldValue),
    LessThanOrEqual(FieldValue),
}

pub trait Delta {
    type Value;

    fn change(&self) -> DeltaChange;

    fn apply_data(&self, value: &mut Self::Value);
}

type BoxedDelta<T> = Box<dyn Delta<Value = T>>;

#[derive(PartialEq, Eq)]
pub struct DeltaScope {
    id: DataItemId,
    field_name: String,
}

pub struct DeltaChange {
    scope: DeltaScope,
    before: Option<FieldValue>,
    after: Option<FieldValue>,
}

impl DeltaChange {
    pub fn new(id: DataItemId, field_name: String) -> Self {
        DeltaChange {
            scope: DeltaScope { id, field_name },
            before: None,
            after: None,
        }
    }

    pub fn before(mut self, before: FieldValue) -> Self {
        self.before = Some(before);
        self
    }

    pub fn after(mut self, after: FieldValue) -> Self {
        self.after = Some(after);
        self
    }
}
