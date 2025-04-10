use std::collections::BTreeMap;
use std::fmt::Display;

use pest::iterators::Pair;
use pest::Parser;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::Date;

use crate::data::{parse_date, DataItem, DataItemId, FieldValue};
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
    pub(crate) branch: Option<u32>,
    pub(crate) date: Date,
}

impl DeltaScope {
    pub fn new(branch: Option<u32>, date: Date) -> Self {
        Self { branch, date }
    }

    pub fn date(date: Date) -> Self {
        DeltaScope { branch: None, date }
    }

    pub fn branch(branch: u32, date: Date) -> Self {
        DeltaScope {
            branch: Some(branch),
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
                    return Err(QueryError::Filter(FilterError::MissingIndex(
                        filter.name.to_string(),
                    )));
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
            .ok_or_else(|| QueryError::Filter(FilterError::MissingIndex(sort.by.to_string())))?;

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

#[derive(Debug, Default)]
pub struct OptionsQueryExecution {
    pub(crate) entity: String,
    filter: Option<CompositeFilter>,
    scope: Option<DeltaScope>,
}

impl OptionsQueryExecution {
    pub fn new() -> Self {
        OptionsQueryExecution::default()
    }

    pub fn parse_query(query: &str) -> Result<Self, ParseError> {
        let parsed = QueryParser::parse_query(query)?;
        Ok(OptionsQueryExecution {
            entity: parsed.entity,
            filter: parsed.filter,
            scope: parsed.scope,
        })
    }

    pub fn for_entity(mut self, entity: String) -> Self {
        self.entity = entity;
        self
    }

    pub fn with_filter(mut self, filter: CompositeFilter) -> Self {
        self.filter = Some(filter);

        self
    }

    pub fn with_scope(mut self, scope: DeltaScope) -> Self {
        self.scope = Some(scope);
        self
    }

    pub fn run(self, storage: &EntityStorage) -> Result<Vec<FilterOption>, QueryError> {
        // Read the indices from storage.
        let indices = match &self.scope {
            Some(scope) => storage.read_all_indices_in(scope)?,
            None => storage.read_all_current_indices()?,
        };

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
    pub(crate) entity: String,
    filter: Option<CompositeFilter>,
    sort: Option<Sort>,
    scope: Option<DeltaScope>,
    pagination: Pagination,
    ref_fields: Vec<String>,
}

impl QueryExecution {
    pub fn new() -> Self {
        QueryExecution::default()
    }

    pub fn parse_query(query: &str) -> Result<Self, ParseError> {
        let parsed = QueryParser::parse_query(query)?;

        let mut ref_fields = Vec::new();
        if let Some(filter) = parsed.filter.as_ref() {
            ref_fields.extend(filter.get_referenced_fields());
        }
        if let Some(sort) = parsed.sort.as_ref() {
            ref_fields.extend(sort.get_referenced_fields());
        }

        Ok(QueryExecution {
            entity: parsed.entity,
            filter: parsed.filter,
            sort: parsed.sort,
            scope: parsed.scope,
            pagination: parsed.pagination,
            ref_fields,
        })
    }

    pub fn for_entity(mut self, entity: String) -> Self {
        self.entity = entity;
        self
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
        self.pagination = pagination;
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
        let paginated_ids = sorted_ids
            .iter()
            .skip(self.pagination.start)
            .take(self.pagination.size);

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

/// A composite filter allows to combine multiple filter expressions using
/// logical conjunction.
#[derive(Debug, PartialEq, Clone)]
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

    pub fn contains(name: &str, value: FieldValue) -> Self {
        CompositeFilter::Single(Filter {
            name: name.to_string(),
            operation: FilterOperation::Contains(value),
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

pub const DEFAULT_START_PAGE: usize = 0;
pub const DEFAULT_PAGE_SIZE: usize = 500;

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct Pagination {
    start: usize,
    size: usize,
}

impl Pagination {
    pub fn new(start: usize, size: usize) -> Self {
        Pagination { start, size }
    }
}

impl Default for Pagination {
    fn default() -> Self {
        Pagination::new(DEFAULT_START_PAGE, DEFAULT_PAGE_SIZE)
    }
}

#[derive(Debug, PartialEq)]
pub enum SortDirection {
    ASC,
    DESC,
}

#[derive(Debug, PartialEq)]
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

/// A single filter expression with a `name` identifying the field to match
/// the filter against, and the filter operation.
#[derive(Debug, PartialEq, Clone)]
pub struct Filter {
    name: String,
    operation: FilterOperation,
}

/// A filter operation collects all the available filter operations.
#[derive(Debug, PartialEq, Clone)]
pub enum FilterOperation {
    Eq(FieldValue),
    Between(FieldValue, FieldValue),
    GreaterThan(FieldValue),
    GreaterOrEqual(FieldValue),
    LessThan(FieldValue),
    LessThanOrEqual(FieldValue),
    Contains(FieldValue),
}

#[derive(Clone, Debug)]
pub enum FilterName {
    Eq,
    Between,
    GreaterThan,
    GreaterOrEqual,
    LessThan,
    LessThanOrEqual,
    Contains,
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
            FilterName::Contains => write!(f, "contains"),
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

#[derive(Debug, PartialEq)]
pub(crate) struct ParsedQuery {
    entity: String,
    scope: Option<DeltaScope>,
    filter: Option<CompositeFilter>,
    sort: Option<Sort>,
    pagination: Pagination,
}

// TODO: implement parsing for "contains"
#[derive(pest_derive::Parser)]
#[grammar_inline = r#"
    WHITESPACE = _{ " " }
    NAME_CHAR  = _{ ASCII_ALPHA | ASCII_DIGIT | "." | "_" }
    name       = @{ NAME_CHAR+ }
    number     = @{ "-"? ~ ("0" | ASCII_NONZERO_DIGIT ~ ASCII_DIGIT*) ~ ("." ~ ASCII_DIGIT*)? ~ (^"e" ~ ("+" | "-")? ~ ASCII_DIGIT+)? }
    string     = ${ "\"" ~ char* ~ "\"" }
    char       =  {
        !("\"" | "\\") ~ ANY
      | "\\" ~ ("\"" | "\\" | "/" | "b" | "f" | "n" | "r" | "t")
      | "\\" ~ ("u" ~ ASCII_HEX_DIGIT{4})
    }
    boolean    =  { ^"TRUE" | ^"FALSE" }
    array      =  { "[" ~ "]" | "[" ~ value ~ ("," ~ value)* ~ "]" }
    date       =  { "\"" ~ ASCII_DIGIT{4} ~ "-" ~ ASCII_DIGIT{2} ~ "-" ~ ASCII_DIGIT{2} ~ "\"" }
    value      =  { number | string | boolean | array }

    eq_operator         = { "=" }
    not_eq_operator     = { "!=" }
    ge_operator         = { ">=" }
    le_operator         = { "<=" }
    gt_operator         = { ">" }
    lt_operator         = { "<" }
    contains_operator   = { ^"CONTAINS" }
    comparison_operator = { eq_operator | not_eq_operator | ge_operator | le_operator | gt_operator | lt_operator | contains_operator }
    logical_operator    = { ^"AND" | ^"OR" }

    ASC  = { ^"ASC" }
    DESC = { ^"DESC" }

    FROM     = { ^"FROM" ~ name }
    WHERE    = { ^"WHERE" ~ composite }
    ORDER_BY = { ^"ORDER BY" ~ name ~ (ASC | DESC)? }
    LIMIT    = { ^"LIMIT" ~ number }
    OFFSET   = { ^"OFFSET" ~ number }
    AS_OF    = { ^"AS OF" ~ date }
    BRANCH    = { ^"BRANCH" ~ number }

    statement = { "("{0, 1} ~ name ~ comparison_operator ~ value ~ ")"{0, 1} }
    composite = { "("{0, 1} ~ statement ~ (logical_operator ~ composite)* ~ ")"{0, 1} }

    // Allow any order of OFFSET and LIMIT
    query     = { FROM ~ WHERE? ~ BRANCH? ~ AS_OF? ~ ORDER_BY? ~ OFFSET? ~ LIMIT? ~ OFFSET?  }
"#]
pub(crate) struct QueryParser;

impl QueryParser {
    pub(crate) fn parse_query(input: &str) -> Result<ParsedQuery, ParseError> {
        let input = Self::normalize(input);
        let mut pairs = Self::parse(Rule::query, &input)
            .map_err(|err| ParseError::QueryParse(err.line().to_string()))?;

        let query_pair = pairs.next().ok_or(ParseError::EmptyQuery)?;
        let mut pairs = query_pair.into_inner();

        let from_pair = pairs.next().ok_or(ParseError::InvalidQuery(
            "query must start with a FROM statement",
        ))?;

        let entity = Self::parse_from(from_pair)?;
        let mut filter = None;
        let mut sort = None;
        let mut start = None;
        let mut size = None;
        let mut delta_scope_date = None;
        let mut delta_scope_branch = None;

        for pair in pairs {
            match pair.as_rule() {
                Rule::WHERE => {
                    filter = Self::parse_where(pair)?;
                }
                Rule::ORDER_BY => {
                    sort = Self::parse_sort(pair)?;
                }
                Rule::LIMIT => {
                    let mut inner = pair.into_inner();
                    size = if let Some(limit) = inner.next() {
                        Some(limit.as_str().parse::<usize>().map_err(|_| {
                            ParseError::InvalidQuery("expected numeric value after LIMIT statement")
                        })?)
                    } else {
                        None
                    };
                }
                Rule::OFFSET => {
                    let mut inner = pair.into_inner();
                    start = if let Some(offset) = inner.next() {
                        Some(offset.as_str().parse::<usize>().map_err(|_| {
                            ParseError::InvalidQuery(
                                "expected numeric value after OFFSET statement",
                            )
                        })?)
                    } else {
                        None
                    };
                }
                Rule::AS_OF => {
                    let mut inner = pair.into_inner();
                    delta_scope_date = if let Some(date) = inner.next() {
                        let Ok(date) = date
                            .as_str()
                            // Remove double quotes from beginning and end (as stated in the grammar)
                            .trim_start_matches('"')
                            .trim_end_matches('"')
                            .parse::<String>();

                        Some(parse_date(&date).map_err(|_| {
                            ParseError::InvalidQuery(
                                "date value has the wrong formatting for AS OF statement",
                            )
                        })?)
                    } else {
                        None
                    };
                }
                Rule::BRANCH => {
                    let mut inner = pair.into_inner();
                    delta_scope_branch = if let Some(branch) = inner.next() {
                        Some(branch.as_str().parse::<u32>().map_err(|_| {
                            ParseError::InvalidQuery(
                                "expected numeric value after BRANCH statement",
                            )
                        })?)
                    } else {
                        None
                    };
                }
                _ => {}
            }
        }

        let pagination = Pagination::new(
            start.unwrap_or(DEFAULT_START_PAGE),
            size.unwrap_or(DEFAULT_PAGE_SIZE),
        );

        let scope = delta_scope_date.map(|date| DeltaScope {
            date,
            branch: delta_scope_branch,
        });

        Ok(ParsedQuery {
            entity,
            filter,
            scope,
            sort,
            pagination,
        })
    }

    fn parse_from(pair: Pair<Rule>) -> Result<String, ParseError> {
        if let Rule::FROM = pair.as_rule() {
            let mut inner = pair.into_inner();

            let entity = inner
                .next()
                .ok_or(ParseError::InvalidQuery(
                    "expected entity name in FROM statement",
                ))?
                .as_str()
                .to_string();

            return Ok(entity);
        }

        Err(ParseError::InvalidQuery(
            "query must start with a FROM statement",
        ))
    }

    fn parse_where(pair: Pair<Rule>) -> Result<Option<CompositeFilter>, ParseError> {
        if let Rule::WHERE = pair.as_rule() {
            let mut inner = pair.into_inner();

            let filter_statement = inner.next().ok_or(ParseError::InvalidQuery(
                "expected filter after WHERE statement",
            ))?;

            let filter = Self::parse_filter_statement(filter_statement)?;
            return Ok(Some(filter));
        }

        Ok(None)
    }

    fn parse_sort(pair: Pair<Rule>) -> Result<Option<Sort>, ParseError> {
        if let Rule::ORDER_BY = pair.as_rule() {
            let mut inner = pair.into_inner();

            let by = inner
                .next()
                .ok_or(ParseError::InvalidQuery(
                    "expected field in ORDER BY statement",
                ))?
                .as_str();

            let mut sort = Sort::new(by);

            if let Some(direction) = inner.next() {
                sort = match direction.as_rule() {
                    Rule::ASC => sort.with_direction(SortDirection::ASC),
                    Rule::DESC => sort.with_direction(SortDirection::DESC),
                    _ => unreachable!(),
                };
            }

            return Ok(Some(sort));
        }

        Ok(None)
    }

    fn parse_filter_statement(pair: Pair<Rule>) -> Result<CompositeFilter, ParseError> {
        match pair.as_rule() {
            Rule::WHITESPACE
            | Rule::NAME_CHAR
            | Rule::name
            | Rule::number
            | Rule::char
            | Rule::string
            | Rule::boolean
            | Rule::date
            | Rule::array
            | Rule::value
            | Rule::comparison_operator
            | Rule::eq_operator
            | Rule::not_eq_operator
            | Rule::ge_operator
            | Rule::le_operator
            | Rule::gt_operator
            | Rule::lt_operator
            | Rule::contains_operator
            | Rule::logical_operator
            | Rule::FROM
            | Rule::WHERE
            | Rule::ORDER_BY
            | Rule::LIMIT
            | Rule::OFFSET
            | Rule::AS_OF
            | Rule::BRANCH
            | Rule::ASC
            | Rule::DESC
            | Rule::query => unreachable!(),
            Rule::statement => {
                let mut inner = pair.into_inner();

                let name = inner
                    .next()
                    .ok_or(ParseError::InvalidQuery(
                        "expected property name in filter statement",
                    ))?
                    .as_str();

                let operator = inner.next().ok_or(ParseError::InvalidQuery(
                    "expected comparison operator in filter statement",
                ))?;

                let value = inner.next().ok_or(ParseError::InvalidQuery(
                    "expected value in filter statement",
                ))?;

                let value = Self::parse_value(value);

                if let Rule::comparison_operator = operator.as_rule() {
                    let operator = operator
                        .into_inner()
                        .next()
                        .ok_or(ParseError::InvalidQuery(
                            "expected comparison operator in filter statement",
                        ))?
                        .as_rule();

                    match operator {
                        Rule::eq_operator => Ok(CompositeFilter::eq(name, value)),
                        Rule::not_eq_operator => {
                            Ok(CompositeFilter::negate(CompositeFilter::eq(name, value)))
                        }
                        Rule::ge_operator => Ok(CompositeFilter::ge(name, value)),
                        Rule::le_operator => Ok(CompositeFilter::le(name, value)),
                        Rule::gt_operator => Ok(CompositeFilter::gt(name, value)),
                        Rule::lt_operator => Ok(CompositeFilter::lt(name, value)),
                        Rule::contains_operator => Ok(CompositeFilter::contains(name, value)),
                        _ => Err(ParseError::UnknownOperator),
                    }
                } else {
                    Err(ParseError::InvalidQuery(
                        "expected operator to be a comparison operator in filter statement",
                    ))
                }
            }
            Rule::composite => {
                let mut inner = pair.into_inner();

                let left = inner.next().ok_or(ParseError::InvalidQuery(
                    "expected left statement in composite rule",
                ))?;

                let Some(operator) = inner.next() else {
                    return Self::parse_filter_statement(left);
                };

                let right = inner.next().ok_or(ParseError::InvalidQuery(
                    "expected right statement in composite rule",
                ))?;

                let left = Self::parse_filter_statement(left)?;
                let right = Self::parse_filter_statement(right)?;

                let operator = operator.as_str();
                match operator {
                    "AND" => Ok(CompositeFilter::And(vec![left, right])),
                    "OR" => Ok(CompositeFilter::Or(vec![left, right])),
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
            | Rule::date
            | Rule::comparison_operator
            | Rule::eq_operator
            | Rule::not_eq_operator
            | Rule::ge_operator
            | Rule::le_operator
            | Rule::gt_operator
            | Rule::lt_operator
            | Rule::contains_operator
            | Rule::logical_operator
            | Rule::statement
            | Rule::composite
            | Rule::FROM
            | Rule::WHERE
            | Rule::ORDER_BY
            | Rule::LIMIT
            | Rule::OFFSET
            | Rule::AS_OF
            | Rule::BRANCH
            | Rule::ASC
            | Rule::DESC
            | Rule::query => unreachable!(),
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

    /// Normalizes an input query to remove duplicate whitespaces, tabs and new lines
    fn normalize(input: &str) -> String {
        let mut result = String::new();
        let mut prev_was_whitespace = false;

        for c in input.chars() {
            if c.is_whitespace() {
                if !prev_was_whitespace {
                    result.push(' ');
                    prev_was_whitespace = true;
                }
            } else {
                result.push(c);
                prev_was_whitespace = false;
            }
        }

        result.trim().to_string()
    }
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum QueryError {
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
    use time::{Date, Month};

    use crate::data::FieldValue;
    use crate::query::{
        CompositeFilter, DeltaScope, Pagination, ParsedQuery, QueryParser, Sort, SortDirection,
        DEFAULT_PAGE_SIZE, DEFAULT_START_PAGE,
    };

    #[test]
    fn creates_skeleton_query() {
        // given
        let input = "FROM person";

        // when
        let result = QueryParser::parse_query(input).unwrap();

        // then
        assert_eq!(
            result,
            ParsedQuery {
                entity: "person".to_string(),
                filter: None,
                sort: None,
                scope: None,
                pagination: Pagination::default()
            }
        )
    }

    #[test]
    fn creates_string_filter() {
        // given
        let input = "FROM person WHERE person.name = \"David\"";

        // when
        let result = QueryParser::parse_query(input).unwrap();

        // then
        assert_eq!(
            result,
            ParsedQuery {
                entity: "person".to_string(),
                filter: Some(CompositeFilter::eq("person.name", FieldValue::str("David"))),
                sort: None,
                scope: None,
                pagination: Pagination::default()
            }
        )
    }

    #[test]
    fn creates_date_filter() {
        // given
        let input = "FROM person WHERE person.birth_date < \"2020-01-01\"";

        // when
        let result = QueryParser::parse_query(input).unwrap();

        // then
        assert_eq!(
            result,
            ParsedQuery {
                entity: "person".to_string(),
                filter: Some(CompositeFilter::lt(
                    "person.birth_date",
                    FieldValue::str("2020-01-01")
                )),
                sort: None,
                scope: None,
                pagination: Pagination::default()
            }
        )
    }

    #[test]
    fn creates_contains_filter() {
        // given
        let input = "FROM person WHERE person.name contains \"Alice\"";

        // when
        let result = QueryParser::parse_query(input).unwrap();

        // then
        assert_eq!(
            result,
            ParsedQuery {
                entity: "person".to_string(),
                filter: Some(CompositeFilter::contains(
                    "person.name",
                    FieldValue::str("Alice")
                )),
                sort: None,
                scope: None,
                pagination: Pagination::default()
            }
        )
    }

    #[test]
    fn creates_complex_filter() {
        // given
        let input = r#"
            FROM person
                WHERE (person.name != "Michael Jordan" AND person.address CONTAINS "Street")
                    AND (score > 1 OR active = true AND (person.name.simple = "Roger" OR score <= 5))
        "#;

        // when
        let result = QueryParser::parse_query(input).unwrap();

        // then
        assert_eq!(
            result,
            ParsedQuery {
                entity: "person".to_string(),
                filter: Some(CompositeFilter::and(vec![
                    CompositeFilter::negate(CompositeFilter::eq(
                        "person.name",
                        FieldValue::str("Michael Jordan")
                    )),
                    CompositeFilter::and(vec![
                        CompositeFilter::contains("person.address", FieldValue::str("Street")),
                        CompositeFilter::or(vec![
                            CompositeFilter::gt("score", FieldValue::dec(1.0)),
                            CompositeFilter::and(vec![
                                CompositeFilter::eq("active", FieldValue::bool(true)),
                                CompositeFilter::or(vec![
                                    CompositeFilter::eq(
                                        "person.name.simple",
                                        FieldValue::str("Roger")
                                    ),
                                    CompositeFilter::le("score", FieldValue::dec(5.0)),
                                ])
                            ])
                        ])
                    ])
                ])),
                sort: None,
                scope: None,
                pagination: Pagination::default()
            }
        )
    }

    #[test]
    fn creates_filter_order_by() {
        // given
        let input = "FROM person WHERE person.name = \"David\" ORDER BY person.score";

        // when
        let result = QueryParser::parse_query(input).unwrap();

        // then
        assert_eq!(
            result,
            ParsedQuery {
                entity: "person".to_string(),
                filter: Some(CompositeFilter::eq("person.name", FieldValue::str("David"))),
                scope: None,
                sort: Some(Sort::new("person.score")),
                pagination: Pagination::default()
            }
        )
    }

    #[test]
    fn creates_filter_order_by_desc() {
        // given
        let input = "FROM person WHERE person.name = \"David\" ORDER BY person.score DESC";

        // when
        let result = QueryParser::parse_query(input).unwrap();

        // then
        assert_eq!(
            result,
            ParsedQuery {
                entity: "person".to_string(),
                filter: Some(CompositeFilter::eq("person.name", FieldValue::str("David"))),
                sort: Some(Sort::new("person.score").with_direction(SortDirection::DESC)),
                scope: None,
                pagination: Pagination::default()
            }
        )
    }

    #[test]
    fn creates_filter_order_by_desc_limit() {
        // given
        let input = "FROM person WHERE person.name = \"David\" ORDER BY person.score DESC LIMIT 10";

        // when
        let result = QueryParser::parse_query(input).unwrap();

        // then
        assert_eq!(
            result,
            ParsedQuery {
                entity: "person".to_string(),
                filter: Some(CompositeFilter::eq("person.name", FieldValue::str("David"))),
                sort: Some(Sort::new("person.score").with_direction(SortDirection::DESC)),
                scope: None,
                pagination: Pagination::new(DEFAULT_START_PAGE, 10)
            }
        )
    }

    #[test]
    fn creates_filter_order_by_asc_offset() {
        // given
        let input = "FROM person WHERE person.name = \"David\" ORDER BY person.score ASC OFFSET 10";

        // when
        let result = QueryParser::parse_query(input).unwrap();

        // then
        assert_eq!(
            result,
            ParsedQuery {
                entity: "person".to_string(),
                filter: Some(CompositeFilter::eq("person.name", FieldValue::str("David"))),
                sort: Some(Sort::new("person.score").with_direction(SortDirection::ASC)),
                scope: None,
                pagination: Pagination::new(10, DEFAULT_PAGE_SIZE)
            }
        )
    }

    #[test]
    fn creates_filter_order_by_offset_limit() {
        // given
        let input =
            "FROM person WHERE person.name = \"David\" ORDER BY person.score OFFSET 10 LIMIT 20";

        // when
        let result = QueryParser::parse_query(input).unwrap();

        // then
        assert_eq!(
            result,
            ParsedQuery {
                entity: "person".to_string(),
                filter: Some(CompositeFilter::eq("person.name", FieldValue::str("David"))),
                sort: Some(Sort::new("person.score").with_direction(SortDirection::ASC)),
                scope: None,
                pagination: Pagination::new(10, 20)
            }
        )
    }

    #[test]
    fn creates_filter_order_by_limit_offset() {
        // given
        let input =
            "FROM person WHERE person.name = \"David\" ORDER BY person.score LIMIT 20 OFFSET 10";

        // when
        let result = QueryParser::parse_query(input).unwrap();

        // then
        assert_eq!(
            result,
            ParsedQuery {
                entity: "person".to_string(),
                filter: Some(CompositeFilter::eq("person.name", FieldValue::str("David"))),
                sort: Some(Sort::new("person.score").with_direction(SortDirection::ASC)),
                scope: None,
                pagination: Pagination::new(10, 20)
            }
        )
    }

    #[test]
    fn creates_filter_as_of_order_by_limit_offset() {
        // given
        let input =
            "FROM person WHERE person.name = \"David\" AS OF \"2020-01-01\" ORDER BY person.score LIMIT 20 OFFSET 10";

        // when
        let result = QueryParser::parse_query(input).unwrap();

        // then
        assert_eq!(
            result,
            ParsedQuery {
                entity: "person".to_string(),
                filter: Some(CompositeFilter::eq("person.name", FieldValue::str("David"))),
                sort: Some(Sort::new("person.score").with_direction(SortDirection::ASC)),
                scope: Some(DeltaScope {
                    date: Date::from_calendar_date(2020, Month::January, 1).unwrap(),
                    branch: None
                }),
                pagination: Pagination::new(10, 20)
            }
        )
    }

    #[test]
    fn creates_filter_as_of_branch_order_by_limit_offset() {
        // given
        let input = r#"
            FROM person
                WHERE person.name = "David"
                BRANCH 1 AS OF "2020-01-01"
                ORDER BY person.score
                LIMIT 20
                OFFSET 10
        "#;

        // when
        let result = QueryParser::parse_query(input).unwrap();

        // then
        assert_eq!(
            result,
            ParsedQuery {
                entity: "person".to_string(),
                filter: Some(CompositeFilter::eq("person.name", FieldValue::str("David"))),
                sort: Some(Sort::new("person.score").with_direction(SortDirection::ASC)),
                scope: Some(DeltaScope {
                    date: Date::from_calendar_date(2020, Month::January, 1).unwrap(),
                    branch: Some(1)
                }),
                pagination: Pagination::new(10, 20)
            }
        )
    }

    #[test]
    fn creates_composite_filter_as_of_branch_order_by_limit_offset() {
        // given
        let input = r#"
            FROM person
                WHERE person.name = "David" OR person.address CONTAINS "Street"
                BRANCH 1 AS OF "2020-01-01"
                ORDER BY person.score
                LIMIT 20
                OFFSET 10
        "#;

        // when
        let result = QueryParser::parse_query(input).unwrap();

        // then
        assert_eq!(
            result,
            ParsedQuery {
                entity: "person".to_string(),
                filter: Some(CompositeFilter::or(vec![
                    CompositeFilter::eq("person.name", FieldValue::str("David")),
                    CompositeFilter::contains("person.address", FieldValue::str("Street"))
                ])),
                sort: Some(Sort::new("person.score").with_direction(SortDirection::ASC)),
                scope: Some(DeltaScope {
                    date: Date::from_calendar_date(2020, Month::January, 1).unwrap(),
                    branch: Some(1)
                }),
                pagination: Pagination::new(10, 20)
            }
        )
    }
}
