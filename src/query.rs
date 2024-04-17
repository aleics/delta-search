use std::collections::HashMap;

use pest::iterators::Pair;
use pest::Parser;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};

use crate::data::{DataItem, DataItemId, FieldValue};
use crate::index::Index;
use crate::storage::{id_to_position, position_to_id, EntityIndices, EntityStorage};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct FilterOption {
    pub field: String,
    pub values: HashMap<String, u64>,
}

impl FilterOption {
    pub(crate) fn new(field: String, values: HashMap<String, u64>) -> Self {
        FilterOption { field, values }
    }
}

#[derive(Debug)]
struct QueryIndices {
    stored: EntityIndices,
    deltas: HashMap<String, Index>,
}

impl QueryIndices {
    fn new(stored: EntityIndices) -> Self {
        QueryIndices {
            stored,
            deltas: HashMap::new(),
        }
    }

    fn attach_deltas(mut self, deltas: &[DeltaChange]) -> Self {
        for delta in deltas {
            // Clone the existing index into the `deltas` related index
            if let Some(current) = self.stored.field_indices.get(&delta.scope.field_name) {
                if !self.deltas.contains_key(&delta.scope.field_name) {
                    self.deltas
                        .insert(delta.scope.field_name.to_string(), current.clone());
                }
            }

            // Apply the change to the delta related index
            if let Some(delta_index) = self.deltas.get_mut(&delta.scope.field_name) {
                let position = id_to_position(delta.scope.id);
                if let Some(before) = delta.before.as_ref() {
                    delta_index.remove(before, position);
                }

                if let Some(after) = delta.after.as_ref() {
                    delta_index.put(after.clone(), position);
                }
            }
        }
        self
    }

    fn get(&self, name: &String) -> Option<&Index> {
        self.deltas
            .get(name)
            .or_else(|| self.stored.field_indices.get(name))
    }

    fn execute_filter(&self, filter: &CompositeFilter) -> FilterResult {
        match filter {
            CompositeFilter::And(filters) => {
                let result: Option<FilterResult> = filters.iter().fold(None, |acc, filter| {
                    let inner = acc
                        .map(|current| current.and(self.execute_filter(filter)))
                        .unwrap_or_else(|| self.execute_filter(filter));
                    Some(inner)
                });

                result.unwrap_or_else(FilterResult::empty)
            }
            CompositeFilter::Or(filters) => {
                let result: Option<FilterResult> = filters.iter().fold(None, |acc, filter| {
                    let inner = acc
                        .map(|current| current.or(self.execute_filter(filter)))
                        .unwrap_or_else(|| self.execute_filter(filter));
                    Some(inner)
                });

                result.unwrap_or_else(FilterResult::empty)
            }
            CompositeFilter::Not(filter) => {
                let result = self.execute_filter(filter);
                FilterResult::new(&self.stored.all - result.hits)
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

        index.sort(items, &sort.direction)
    }

    fn compute_filter_options(&self, hits: RoaringBitmap) -> Vec<FilterOption> {
        let mut filter_options = Vec::new();

        for (field, index) in &self.deltas {
            filter_options.push(FilterOption::new(field.to_string(), index.counts(&hits)));
        }

        for (field, index) in &self.stored.field_indices {
            if !self.deltas.contains_key(field) {
                filter_options.push(FilterOption::new(field.to_string(), index.counts(&hits)))
            }
        }

        filter_options
    }
}

#[derive(Default)]
pub struct OptionsQueryExecution {
    filter: Option<CompositeFilter>,
    deltas: Vec<DeltaChange>,
    ref_fields: Option<Vec<String>>,
}

impl OptionsQueryExecution {
    pub fn new() -> Self {
        OptionsQueryExecution::default()
    }

    pub fn with_filter(mut self, filter: CompositeFilter) -> Self {
        if let Some(ref_fields) = self.ref_fields.as_mut() {
            ref_fields.append(&mut filter.get_referenced_fields());
        }
        self.filter = Some(filter);

        self
    }

    pub fn with_deltas(mut self, deltas: Vec<DeltaChange>) -> Self {
        self.deltas.extend(deltas);
        self
    }

    pub fn run(self, storage: &EntityStorage) -> Vec<FilterOption> {
        // Read the indices from storage. In case no fields are referenced, use all indices
        // as filter options.
        let indices = match self.ref_fields {
            Some(fields) => storage.read_indices(fields.as_slice()),
            None => storage.read_all_indices(),
        };

        let indices = QueryIndices::new(indices).attach_deltas(&self.deltas);

        let filter_result = self
            .filter
            .as_ref()
            .map(|filter| indices.execute_filter(filter))
            .unwrap_or_else(|| FilterResult::new(indices.stored.all.clone()));

        indices.compute_filter_options(filter_result.hits)
    }
}

#[derive(Default)]
pub struct QueryExecution {
    filter: Option<CompositeFilter>,
    deltas: Vec<DeltaChange>,
    sort: Option<Sort>,
    pagination: Option<Pagination>,
    ref_fields: Vec<String>,
}

impl QueryExecution {
    pub fn new() -> Self {
        QueryExecution::default()
    }

    pub fn with_filter(mut self, filter: CompositeFilter) -> Self {
        self.ref_fields.append(&mut filter.get_referenced_fields());
        self.filter = Some(filter);
        self
    }

    pub fn with_deltas(mut self, deltas: Vec<DeltaChange>) -> Self {
        self.deltas.extend(deltas);
        self
    }

    pub fn with_sort(mut self, sort: Sort) -> Self {
        self.ref_fields.append(&mut sort.get_referenced_fields());
        self.sort = Some(sort);
        self
    }

    pub fn with_pagination(mut self, pagination: Pagination) -> Self {
        self.pagination = Some(pagination);
        self
    }

    pub fn run(self, storage: &EntityStorage) -> Vec<DataItem> {
        let indices =
            QueryIndices::new(storage.read_indices(&self.ref_fields)).attach_deltas(&self.deltas);

        let filter_result = self
            .filter
            .as_ref()
            .map(|filter| indices.execute_filter(filter))
            .unwrap_or_else(|| FilterResult::new(indices.stored.all.clone()));

        let item_ids = self.sort(filter_result, &indices);

        self.read_data(&item_ids, storage)
    }

    fn sort(&self, filter_result: FilterResult, indices: &QueryIndices) -> Vec<DataItemId> {
        if let Some(sort) = &self.sort {
            return indices
                .execute_sort(&filter_result.hits, sort)
                .iter()
                .copied()
                .map(position_to_id)
                .collect();
        }

        filter_result.hits.iter().map(position_to_id).collect()
    }

    fn read_data(&self, ids: &[DataItemId], storage: &EntityStorage) -> Vec<DataItem> {
        let mut data = Vec::new();

        let deltas_by_id: HashMap<DataItemId, Vec<&DeltaChange>> =
            self.deltas.iter().fold(HashMap::new(), |mut acc, delta| {
                let key = delta.scope.id;
                acc.entry(key).or_default().push(delta);
                acc
            });

        let pagination = self.pagination.unwrap_or(Pagination::new(0, ids.len()));

        for id in ids.iter().skip(pagination.start).take(pagination.size) {
            let Some(mut item) = storage.read_by_id(id) else {
                continue;
            };

            if let Some(deltas) = deltas_by_id.get(id) {
                for delta in deltas {
                    let Some(after) = delta.after.as_ref() else {
                        continue;
                    };

                    item.fields
                        .insert(delta.scope.field_name.clone(), after.clone());
                }
            }

            data.push(item);
        }

        data
    }
}

#[derive(Debug, PartialEq)]
pub enum CompositeFilter {
    And(Vec<CompositeFilter>),
    Or(Vec<CompositeFilter>),
    Not(Box<CompositeFilter>),
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

    pub fn negate(filter: CompositeFilter) -> Self {
        CompositeFilter::Not(Box::new(filter))
    }

    pub fn get_referenced_fields(&self) -> Vec<String> {
        match self {
            CompositeFilter::And(composite) | CompositeFilter::Or(composite) => composite
                .iter()
                .flat_map(|filter| filter.get_referenced_fields())
                .collect(),
            CompositeFilter::Not(filter) => filter.get_referenced_fields(),
            CompositeFilter::Single(filter) => vec![filter.name.to_string()],
        }
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

pub enum SortDirection {
    ASC,
    DESC,
}

pub struct Sort {
    by: String,
    direction: SortDirection,
}

impl Sort {
    pub fn new(by: &str) -> Self {
        Sort {
            by: by.to_string(),
            direction: SortDirection::ASC,
        }
    }

    pub fn with_direction(mut self, direction: SortDirection) -> Self {
        self.direction = direction;
        self
    }

    fn get_referenced_fields(&self) -> Vec<String> {
        vec![self.by.to_string()]
    }
}

#[derive(Debug, PartialEq)]
pub struct Filter {
    name: String,
    operation: FilterOperation,
}

#[derive(Debug, PartialEq)]
pub enum FilterOperation {
    Eq(FieldValue),
    Between(FieldValue, FieldValue),
    GreaterThan(FieldValue),
    GreaterOrEqual(FieldValue),
    LessThan(FieldValue),
    LessThanOrEqual(FieldValue),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaScope {
    id: DataItemId,
    field_name: String,
}

#[derive(Debug, Clone)]
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

#[derive(pest_derive::Parser)]
#[grammar_inline = r#"
    WHITESPACE = _{ " " }
    NAME_CHAR  = _{ ASCII_ALPHA | "." }
    name       =  { NAME_CHAR+ }
    number     = @{ "-"? ~ ("0" | ASCII_NONZERO_DIGIT ~ ASCII_DIGIT*) ~ ("." ~ ASCII_DIGIT*)? ~ (^"e" ~ ("+" | "-")? ~ ASCII_DIGIT+)? }
    string     =  { "\"" ~ ASCII_ALPHA* ~ "\"" }
    boolean    =  { "true" | "false" }
    array      =  { "[" ~ "]" | "[" ~ value ~ ("," ~ value)* ~ "]" }
    value      =  { number | string | boolean | array }

    comparison_operator = { "=" | "!=" | ">=" | "<=" | ">" | "<" }
    logical_operator    = { "&&" | "||" }

    statement = { "("{0, 1} ~ name ~ SPACE_SEPARATOR* ~ comparison_operator ~ value ~ ")"{0, 1} }
    composite = { "("{0, 1} ~ statement ~ logical_operator* ~ composite* ~ ")"{0, 1} }
"#]
pub(crate) struct FilterParser;

impl FilterParser {
    pub(crate) fn parse_query(input: &str) -> CompositeFilter {
        let mut pairs = Self::parse(Rule::composite, input).unwrap();
        Self::parse_statement(pairs.next().unwrap())
    }

    fn parse_statement(pair: Pair<Rule>) -> CompositeFilter {
        match pair.as_rule() {
            Rule::WHITESPACE
            | Rule::NAME_CHAR
            | Rule::name
            | Rule::number
            | Rule::string
            | Rule::boolean
            | Rule::array
            | Rule::value
            | Rule::comparison_operator
            | Rule::logical_operator => unreachable!(),
            Rule::statement => {
                let mut inner = pair.into_inner();

                let name = inner
                    .next()
                    .expect("Could not find name in statement rule")
                    .as_str();

                let operator = inner
                    .next()
                    .expect("Could not find comparison_operator in statement rule")
                    .as_str();

                let value = inner
                    .next()
                    .expect("Could not find value in statement rule");

                let value = Self::parse_value(value);

                match operator {
                    "=" => CompositeFilter::eq(name, value),
                    ">=" => CompositeFilter::ge(name, value),
                    "<=" => CompositeFilter::le(name, value),
                    ">" => CompositeFilter::gt(name, value),
                    "<" => CompositeFilter::lt(name, value),
                    "!=" => CompositeFilter::negate(CompositeFilter::eq(name, value)),
                    _ => panic!("Unknown comparison operator \"{}\"", operator),
                }
            }
            Rule::composite => {
                let mut inner = pair.into_inner();

                let left = inner
                    .next()
                    .expect("Could not find left statement in composite rule");

                let Some(operator) = inner.next() else {
                    return Self::parse_statement(left);
                };

                let right = inner
                    .next()
                    .expect("Could not right left statement in composite rule");

                let left = Self::parse_statement(left);
                let right = Self::parse_statement(right);

                let operator = operator.as_str();
                match operator {
                    "&&" => CompositeFilter::And(vec![left, right]),
                    "||" => CompositeFilter::Or(vec![left, right]),
                    _ => panic!("Unknown boolean operator \"{}\"", operator),
                }
            }
        }
    }

    fn parse_value(pair: Pair<Rule>) -> FieldValue {
        match pair.as_rule() {
            Rule::WHITESPACE
            | Rule::NAME_CHAR
            | Rule::name
            | Rule::comparison_operator
            | Rule::logical_operator
            | Rule::statement
            | Rule::composite => unreachable!(),
            Rule::value => {
                let value = pair
                    .into_inner()
                    .next()
                    .expect("Value does not include any inner value.");
                Self::parse_value(value)
            }
            Rule::number => {
                let value = pair
                    .as_str()
                    .parse()
                    .expect("Numeric value could not be parsed as f64.");
                FieldValue::dec(value)
            }
            Rule::string => {
                let value = pair
                    .as_str()
                    // Remove double quotes from beginning and end (as stated in the grammar)
                    .trim_start_matches('"')
                    .trim_end_matches('"');
                FieldValue::str(value)
            }
            Rule::boolean => {
                let value = pair
                    .as_str()
                    .parse()
                    .expect("Boolean value could not be parsed as bool.");
                FieldValue::Bool(value)
            }
            Rule::array => {
                let value = pair.into_inner().map(Self::parse_value).collect();
                FieldValue::Array(value)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::data::FieldValue;
    use crate::query::{CompositeFilter, FilterParser};

    #[test]
    fn creates_simple_filter() {
        // given
        let input = "(person.name != \"Michael Jordan\") && (score > 1 || active = true && (person.name.simple = \"Roger\" || score <= 5))";

        // when
        let result = FilterParser::parse_query(input);

        // then
        assert_eq!(
            result,
            CompositeFilter::and(vec![
                CompositeFilter::negate(CompositeFilter::eq(
                    "person.name",
                    FieldValue::str("Michael Jordan")
                )),
                CompositeFilter::or(vec![
                    CompositeFilter::gt("score", FieldValue::dec(1.0)),
                    CompositeFilter::and(vec![
                        CompositeFilter::eq("active", FieldValue::bool(true)),
                        CompositeFilter::or(vec![
                            CompositeFilter::eq("person.name.simple", FieldValue::str("Roger")),
                            CompositeFilter::le("score", FieldValue::dec(5.0)),
                        ])
                    ])
                ])
            ])
        )
    }
}
