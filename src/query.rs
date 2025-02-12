use std::collections::BTreeMap;
use std::fmt::Display;

use pest::iterators::Pair;
use pest::Parser;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::Date;

use crate::data::{DataItem, DataItemId, FieldValue};
use crate::index::{FilterError, Index};
use crate::storage::{position_to_id, EntityIndices, EntityStorage, StorageError};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct FilterOption {
    pub field: String,
    pub values: BTreeMap<String, u64>,
}

impl FilterOption {
    pub(crate) fn new(field: String, values: BTreeMap<String, u64>) -> Self {
        FilterOption { field, values }
    }
}

#[derive(Debug, PartialEq, Deserialize)]
pub struct DeltaScope {
    pub(crate) context: Option<u32>,
    pub(crate) date: Date,
}

impl DeltaScope {
    pub fn new(context: Option<u32>, date: Date) -> Self {
        Self { context, date }
    }

    pub fn date(date: Date) -> Self {
        DeltaScope {
            context: None,
            date,
        }
    }

    pub fn context(context: u32, date: Date) -> Self {
        DeltaScope {
            context: Some(context),
            date,
        }
    }
}

#[derive(Debug)]
struct QueryIndices {
    indices: EntityIndices,
}

impl QueryIndices {
    fn new(indices: EntityIndices) -> Self {
        QueryIndices { indices }
    }

    fn get(&self, name: &String) -> Option<&Index> {
        self.indices.field_indices.get(name)
    }

    fn execute_filter(&self, filter: &CompositeFilter) -> Result<FilterResult, QueryError> {
        let result = match filter {
            CompositeFilter::And(filters) => {
                let mut result: Option<FilterResult> = None;

                for filter in filters {
                    let inner = self.execute_filter(filter)?;
                    let next = if let Some(current) = result {
                        current.and(inner)
                    } else {
                        inner
                    };

                    result = Some(next);
                }

                result.unwrap_or_else(FilterResult::empty)
            }
            CompositeFilter::Or(filters) => {
                let mut result: Option<FilterResult> = None;

                for filter in filters {
                    let inner = self.execute_filter(filter)?;
                    let next = if let Some(current) = result {
                        current.or(inner)
                    } else {
                        inner
                    };

                    result = Some(next);
                }

                result.unwrap_or_else(FilterResult::empty)
            }
            CompositeFilter::Not(filter) => {
                let result = self.execute_filter(filter)?;
                FilterResult::new(&self.indices.all - result.hits)
            }
            CompositeFilter::Single(filter) => {
                let Some(index) = self.get(&filter.name) else {
                    return Err(QueryError::MissingIndex(filter.name.to_string()));
                };

                let hits = index.filter(&filter.operation)?;

                FilterResult::new(hits)
            }
        };

        Ok(result)
    }

    fn execute_sort(&self, items: &RoaringBitmap, sort: &Sort) -> Result<Vec<u32>, QueryError> {
        let index = self
            .get(&sort.by)
            .ok_or_else(|| QueryError::MissingIndex(sort.by.to_string()))?;

        Ok(index.sort(items, &sort.direction))
    }

    fn compute_filter_options(&self, hits: RoaringBitmap) -> Vec<FilterOption> {
        let mut filter_options = Vec::new();

        for (field, index) in &self.indices.field_indices {
            filter_options.push(FilterOption::new(field.to_string(), index.counts(&hits)));
        }

        filter_options
    }
}

#[derive(Default)]
pub struct OptionsQueryExecution {
    filter: Option<CompositeFilter>,
    scope: Option<DeltaScope>,
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

    pub fn with_scope(mut self, scope: DeltaScope) -> Self {
        self.scope = Some(scope);
        self
    }

    pub fn run(self, storage: &EntityStorage) -> Result<Vec<FilterOption>, QueryError> {
        // Read the indices from storage. In case no fields are referenced, use all indices
        // as filter options.
        let indices = match (self.ref_fields, &self.scope) {
            (Some(fields), Some(scope)) => storage.read_indices_in(scope, fields.as_slice()),
            (Some(fields), None) => storage.read_current_indices(fields.as_slice()),
            (None, Some(scope)) => storage.read_all_indices_in(scope),
            (None, None) => storage.read_all_current_indices(),
        }?;

        let indices = QueryIndices::new(indices);

        let filter_result = if let Some(filter) = self.filter.as_ref() {
            indices.execute_filter(filter)?
        } else {
            FilterResult::new(indices.indices.all.clone())
        };

        Ok(indices.compute_filter_options(filter_result.hits))
    }
}

#[derive(Default)]
pub struct QueryExecution {
    filter: Option<CompositeFilter>,
    sort: Option<Sort>,
    scope: Option<DeltaScope>,
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

    pub fn with_sort(mut self, sort: Sort) -> Self {
        self.ref_fields.append(&mut sort.get_referenced_fields());
        self.sort = Some(sort);
        self
    }

    pub fn with_pagination(mut self, pagination: Pagination) -> Self {
        self.pagination = Some(pagination);
        self
    }

    pub fn with_scope(mut self, scope: DeltaScope) -> Self {
        self.scope = Some(scope);
        self
    }

    pub fn run(self, storage: &EntityStorage) -> Result<Vec<DataItem>, QueryError> {
        // Read indices for the referenced fields in the query
        let indices = match &self.scope {
            Some(scope) => storage.read_indices_in(scope, &self.ref_fields),
            None => storage.read_current_indices(&self.ref_fields),
        }?;

        let indices = QueryIndices::new(indices);

        // Apply filter given the indices
        let filter_result = if let Some(filter) = self.filter.as_ref() {
            indices.execute_filter(filter)?
        } else {
            FilterResult::new(indices.indices.all.clone())
        };

        // Sort filter results into a vector of IDs
        let sorted_ids = self.sort(filter_result, &indices)?;

        // Apply pagination
        let pagination = self
            .pagination
            .unwrap_or(Pagination::new(0, sorted_ids.len()));

        let paginated_ids = sorted_ids
            .iter()
            .skip(pagination.start)
            .take(pagination.size);

        // Read from the database the data of the paginated result
        storage
            .read_multiple(paginated_ids, &indices.indices)
            .map_err(QueryError::Storage)
    }

    fn sort(
        &self,
        filter_result: FilterResult,
        indices: &QueryIndices,
    ) -> Result<Vec<DataItemId>, QueryError> {
        let sorted_ids = if let Some(sort) = &self.sort {
            let sort_result = indices.execute_sort(&filter_result.hits, sort)?;
            sort_result.into_iter().map(position_to_id).collect()
        } else {
            filter_result.hits.iter().map(position_to_id).collect()
        };

        Ok(sorted_ids)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum CompositeFilter {
    And(Vec<CompositeFilter>),
    Or(Vec<CompositeFilter>),
    Not(Box<CompositeFilter>),
    Single(Filter),
}

impl CompositeFilter {
    pub fn parse(query: &str) -> Result<CompositeFilter, ParseError> {
        FilterParser::parse_query(query)
    }

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

#[derive(Debug, PartialEq, Clone)]
pub struct Filter {
    name: String,
    operation: FilterOperation,
}

#[derive(Debug, PartialEq, Clone)]
pub enum FilterOperation {
    Eq(FieldValue),
    Between(FieldValue, FieldValue),
    GreaterThan(FieldValue),
    GreaterOrEqual(FieldValue),
    LessThan(FieldValue),
    LessThanOrEqual(FieldValue),
}

#[derive(Clone, Debug)]
pub enum FilterName {
    Eq,
    Between,
    GreaterThan,
    GreaterOrEqual,
    LessThan,
    LessThanOrEqual,
}

impl Display for FilterName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FilterName::Eq => write!(f, "equal"),
            FilterName::Between => write!(f, "between"),
            FilterName::GreaterThan => write!(f, "greater than"),
            FilterName::GreaterOrEqual => write!(f, "greater or equal than"),
            FilterName::LessThan => write!(f, "less than"),
            FilterName::LessThanOrEqual => write!(f, "less than or equal"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DeltaChange {
    pub id: DataItemId,
    pub field_name: String,
    pub after: FieldValue,
}

impl DeltaChange {
    pub fn new(id: DataItemId, field_name: String, after: FieldValue) -> Self {
        DeltaChange {
            id,
            field_name,
            after,
        }
    }
}

#[derive(pest_derive::Parser)]
#[grammar_inline = r#"
    WHITESPACE = _{ " " }
    NAME_CHAR  = _{ ASCII_ALPHA | "." | "_" }
    name       =  { NAME_CHAR+ }
    number     = @{ "-"? ~ ("0" | ASCII_NONZERO_DIGIT ~ ASCII_DIGIT*) ~ ("." ~ ASCII_DIGIT*)? ~ (^"e" ~ ("+" | "-")? ~ ASCII_DIGIT+)? }
    string     = ${ "\"" ~ char* ~ "\"" }
    char       = {
        !("\"" | "\\") ~ ANY
        | "\\" ~ ("\"" | "\\" | "/" | "b" | "f" | "n" | "r" | "t")
        | "\\" ~ ("u" ~ ASCII_HEX_DIGIT{4})
    }
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
    pub fn parse_query(input: &str) -> Result<CompositeFilter, ParseError> {
        let mut pairs = Self::parse(Rule::composite, input)
            .map_err(|err| ParseError::QueryParse(err.line().to_string()))?;
        let query_pair = pairs.next().ok_or(ParseError::EmptyQuery)?;
        Self::parse_statement(query_pair)
    }

    fn parse_statement(pair: Pair<Rule>) -> Result<CompositeFilter, ParseError> {
        match pair.as_rule() {
            Rule::WHITESPACE
            | Rule::NAME_CHAR
            | Rule::name
            | Rule::number
            | Rule::char
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
                    .ok_or(ParseError::InvalidQuery(
                        "expected property name in filter statement",
                    ))?
                    .as_str();

                let operator = inner
                    .next()
                    .ok_or(ParseError::InvalidQuery(
                        "expected comparison operator in filter statement",
                    ))?
                    .as_str();

                let value = inner.next().ok_or(ParseError::InvalidQuery(
                    "expected value in filter statement",
                ))?;

                let value = Self::parse_value(value);

                match operator {
                    "=" => Ok(CompositeFilter::eq(name, value)),
                    ">=" => Ok(CompositeFilter::ge(name, value)),
                    "<=" => Ok(CompositeFilter::le(name, value)),
                    ">" => Ok(CompositeFilter::gt(name, value)),
                    "<" => Ok(CompositeFilter::lt(name, value)),
                    "!=" => Ok(CompositeFilter::negate(CompositeFilter::eq(name, value))),
                    _ => Err(ParseError::UnknownOperator),
                }
            }
            Rule::composite => {
                let mut inner = pair.into_inner();

                let left = inner.next().ok_or(ParseError::InvalidQuery(
                    "expected left statement in composite rule",
                ))?;

                let Some(operator) = inner.next() else {
                    return Self::parse_statement(left);
                };

                let right = inner.next().ok_or(ParseError::InvalidQuery(
                    "expected right statement in composite rule",
                ))?;

                let left = Self::parse_statement(left)?;
                let right = Self::parse_statement(right)?;

                let operator = operator.as_str();
                match operator {
                    "&&" => Ok(CompositeFilter::And(vec![left, right])),
                    "||" => Ok(CompositeFilter::Or(vec![left, right])),
                    _ => Err(ParseError::UnknownOperator),
                }
            }
        }
    }

    fn parse_value(pair: Pair<Rule>) -> FieldValue {
        match pair.as_rule() {
            Rule::WHITESPACE
            | Rule::NAME_CHAR
            | Rule::name
            | Rule::char
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

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum QueryError {
    #[error("index is not present for field \"{0}\"")]
    MissingIndex(String),
    #[error(transparent)]
    Filter(#[from] FilterError),
    #[error(transparent)]
    Storage(#[from] StorageError),
}

#[derive(Error, Debug, PartialEq)]
#[non_exhaustive]
pub enum ParseError {
    #[error("query is defined but empty")]
    EmptyQuery,
    #[error("invalid query \"{0}\"")]
    InvalidQuery(&'static str),
    #[error("query could not be parsed for line \"{0}\"")]
    QueryParse(String),
    #[error("query contains unknown operator")]
    UnknownOperator,
}

#[cfg(test)]
mod tests {
    use crate::data::FieldValue;
    use crate::query::{CompositeFilter, FilterParser};

    #[test]
    fn creates_string_filter() {
        // given
        let input = "person.name = \"David\"";

        // when
        let result = FilterParser::parse_query(input).unwrap();

        // then
        assert_eq!(
            result,
            CompositeFilter::eq("person.name", FieldValue::str("David"))
        )
    }

    #[test]
    fn creates_date_filter() {
        // given
        let input = "person.birth_date < \"2020-01-01\"";

        // when
        let result = FilterParser::parse_query(input);

        // then
        assert_eq!(
            result,
            Ok(CompositeFilter::lt(
                "person.birth_date",
                FieldValue::str("2020-01-01")
            ))
        )
    }

    #[test]
    fn creates_complex_filter() {
        // given
        let input = "(person.name != \"Michael Jordan\") && (score > 1 || active = true && (person.name.simple = \"Roger\" || score <= 5))";

        // when
        let result = FilterParser::parse_query(input).unwrap();

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
