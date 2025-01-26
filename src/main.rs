use std::sync::Arc;

use anyhow::anyhow;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{response::Json, Router};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::format_description::well_known::Iso8601;
use time::Date;

use delta_search::data::{
    DataItem, DataItemFieldsExternal, DataItemId, FieldValue, FieldValueExternal,
};
use delta_search::index::TypeDescriptor;
use delta_search::query::{
    CompositeFilter, DeltaChange, DeltaScope, FilterOption, OptionsQueryExecution, Pagination,
    QueryExecution, Sort, SortDirection,
};
use delta_search::storage::CreateFieldIndex;
use delta_search::Engine;
use tracing::{error, info};

const DEFAULT_START_PAGE: usize = 0;
const DEFAULT_PAGE_SIZE: usize = 500;

#[derive(Clone)]
struct App {
    inner: Arc<Engine>,
}

impl App {
    fn init() -> Result<App, AppError> {
        let engine = Engine::init()
            .inspect_err(|err| error!("Could not initialize engine: {}", err))
            .map_err(|_| anyhow!("Could not initialize engine"))?;

        Ok(App {
            inner: Arc::new(engine),
        })
    }

    fn create_entity(&self, name: &str) -> Result<(), AppError> {
        self.inner
            .create_entity(name.to_string())
            .inspect_err(|err| error!("Could not create entity: {}", err))
            .map_err(|_| anyhow!("Could not create entity `{}`", name))?;

        Ok(())
    }

    fn add_items(&self, name: &str, items: Vec<DataItemExternal>) -> Result<(), AppError> {
        let items: Vec<DataItem> = items
            .into_iter()
            .map(|input_item| DataItem::new(input_item.id, input_item.fields.inner))
            .collect();

        self.inner
            .add_multiple(name, items.as_slice())
            .inspect_err(|err| error!("Could not add items: {}", err))
            .map_err(|_| anyhow!("Could not add items for entity `{}`", name))?;

        Ok(())
    }

    fn add_deltas(
        &self,
        name: &str,
        scope: DeltaScopeInput,
        deltas_input: Vec<DeltaChangeInput>,
    ) -> Result<(), AppError> {
        let mut deltas = Vec::new();
        for delta_input in deltas_input {
            deltas.push(delta_input.map_delta_change()?);
        }

        let scope = scope.map_delta_scope()?;

        self.inner
            .store_deltas(name, &scope, deltas.as_slice())
            .inspect_err(|err| error!("Could not store deltas: {}", err))
            .map_err(|_| anyhow!("Could not store deltas for entity `{}`", name))?;

        Ok(())
    }

    fn query(&self, name: &str, input: QueryIndexInput) -> Result<Vec<DataItemExternal>, AppError> {
        let execution = Self::build_query_execution(input)?;

        self.inner
            .query(name, execution)
            .map(|items| items.into_iter().map(DataItemExternal::from_item).collect())
            .inspect_err(|err| error!("Query could not be executed: {}", err))
            .map_err(|_| anyhow!("Query for entity `{}` could not be executed", name).into())
    }

    fn build_query_execution(input: QueryIndexInput) -> Result<QueryExecution, AppError> {
        let mut execution = QueryExecution::new();

        if let Some(filter) = &input.filter {
            let parsed_filter =
                CompositeFilter::parse(filter).map_err(|_| AppError::InvalidFilterQuery)?;
            execution = execution.with_filter(parsed_filter);
        }

        if let Some(sort) = &input.sort {
            let direction = match sort.direction {
                SortDirectionInput::Asc => SortDirection::ASC,
                SortDirectionInput::Desc => SortDirection::DESC,
            };

            execution = execution.with_sort(Sort::new(&sort.by).with_direction(direction));
        }

        let pagination = input
            .page
            .map(|page| {
                Pagination::new(
                    page.start.unwrap_or(DEFAULT_START_PAGE),
                    page.size.unwrap_or(DEFAULT_PAGE_SIZE),
                )
            })
            .unwrap_or(Pagination::new(DEFAULT_START_PAGE, DEFAULT_PAGE_SIZE));

        if let Some(scope) = input.scope {
            execution = execution.with_scope(scope.map_delta_scope()?);
        }

        Ok(execution.with_pagination(pagination))
    }

    fn options(&self, name: &str) -> Result<Vec<FilterOption>, AppError> {
        self.inner
            .options(name, OptionsQueryExecution::new())
            .inspect_err(|err| error!("Could not create options: {}", err))
            .map_err(|_| anyhow!("Could not create options for entity `{}`", name).into())
    }

    fn create_index(&self, name: &str, input: CreateIndexInput) -> Result<(), AppError> {
        let descriptor = match input.kind {
            CreateIndexTypeInput::String => TypeDescriptor::String,
            CreateIndexTypeInput::Numeric => TypeDescriptor::Numeric,
            CreateIndexTypeInput::Date => TypeDescriptor::Date,
            CreateIndexTypeInput::Bool => TypeDescriptor::Bool,
        };

        let command = CreateFieldIndex {
            name: input.name,
            descriptor,
        };

        self.inner
            .create_index(name, command)
            .inspect_err(|err| error!("Could not create index: {}", err))
            .map_err(|_| anyhow!("Could not create index for entity `{}`", name).into())
    }
}

#[derive(Error, Debug)]
enum AppError {
    #[error("filter query is not valid")]
    InvalidFilterQuery,
    #[error("request is not valid")]
    InvalidRequest { message: String },
    #[error(transparent)]
    ServerError(#[from] anyhow::Error),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        match self {
            AppError::InvalidFilterQuery => Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::new(
                    "Filter query is invalid or could not be parsed.".to_string(),
                ))
                .unwrap(),
            AppError::InvalidRequest { message } => Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::new(format!("Invalid request: \"{}\"", message)))
                .unwrap(),
            AppError::ServerError(err) => Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::new(format!("Something went wrong: {}", err)))
                .unwrap(),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), AppError> {
    tracing_subscriber::fmt::init();
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();

    let search_engine = App::init()?;

    let app = Router::new()
        .route("/entities/{entity_name}", post(create_entity))
        // Storage endpoints
        .route("/data/{entity_name}", put(bulk_upsert_entity))
        .route("/deltas/{entity_name}", post(bulk_add_deltas))
        // Index endpoints
        .route("/indices/{entity_name}", put(create_index))
        // Search endpoints
        .route("/indices/{entity_name}/options", get(get_options))
        .route("/indices/{entity_name}/search", post(query))
        .with_state(search_engine);

    info!("delta-search is running...");

    axum::serve(listener, app).await.unwrap();

    Ok(())
}

async fn create_entity(
    State(search): State<App>,
    Path(name): Path<String>,
) -> Result<Json<()>, AppError> {
    search.create_entity(&name)?;
    Ok(Json(()))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BulkUpsertEntity {
    data: Vec<DataItemExternal>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DataItemExternal {
    id: DataItemId,
    fields: DataItemFieldsExternal,
}

impl DataItemExternal {
    fn from_item(item: DataItem) -> Self {
        DataItemExternal {
            id: item.id,
            fields: DataItemFieldsExternal::new(item.fields),
        }
    }
}

async fn bulk_upsert_entity(
    State(search): State<App>,
    Path(name): Path<String>,
    Json(input): Json<BulkUpsertEntity>,
) -> Result<Json<()>, AppError> {
    search.add_items(&name, input.data)?;
    Ok(Json(()))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BulkStoreDeltas {
    scope: DeltaScopeInput,
    deltas: Vec<DeltaChangeInput>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeltaChangeInput {
    id: DataItemId,
    field_name: String,
    before: FieldValueExternal,
    after: FieldValueExternal,
}

impl DeltaChangeInput {
    fn map_delta_change(self) -> Result<DeltaChange, AppError> {
        let before = DeltaChangeInput::map_field(&self.field_name, self.before)?;
        let after = DeltaChangeInput::map_field(&self.field_name, self.after)?;

        Ok(DeltaChange {
            id: self.id,
            field_name: self.field_name,
            before,
            after,
        })
    }

    fn map_field(name: &String, value: FieldValueExternal) -> Result<FieldValue, AppError> {
        match value {
            FieldValueExternal::Bool(value) => Ok(FieldValue::Bool(value)),
            FieldValueExternal::Integer(value) => Ok(FieldValue::Integer(value)),
            FieldValueExternal::String(value) => Ok(FieldValue::String(value)),
            FieldValueExternal::Decimal(value) => Ok(FieldValue::dec(value)),
            FieldValueExternal::Seq(seq) => {
                let mut values = Vec::new();

                for value in seq {
                    values.push(DeltaChangeInput::map_field(name, value)?);
                }

                Ok(FieldValue::Array(values))
            }
            FieldValueExternal::Map(_) => Err(AppError::InvalidRequest {
                message: format!("Delta field value is invalid for field name {}. Only literals and arrays are allowed.", name),
            }),
        }
    }
}

fn parse_date(string: &str) -> Result<Date, time::error::Parse> {
    Date::parse(string, &Iso8601::DEFAULT)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeltaScopeInput {
    context: Option<u32>,
    date: String,
}

impl DeltaScopeInput {
    fn map_delta_scope(self) -> Result<DeltaScope, AppError> {
        let date = parse_date(&self.date).map_err(|_| AppError::InvalidRequest {
            message: "Date format is invalid. Only ISO 8601 is supported.".to_string(),
        })?;

        Ok(DeltaScope::new(self.context, date))
    }
}

async fn bulk_add_deltas(
    State(search): State<App>,
    Path(name): Path<String>,
    Json(input): Json<BulkStoreDeltas>,
) -> Result<Json<()>, AppError> {
    search.add_deltas(&name, input.scope, input.deltas)?;
    Ok(Json(()))
}

async fn get_options(
    State(search): State<App>,
    Path(name): Path<String>,
) -> Result<Json<Vec<FilterOption>>, AppError> {
    let options = search.options(&name)?;
    Ok(Json(options))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateIndexInput {
    name: String,
    #[serde(rename = "type")]
    kind: CreateIndexTypeInput,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
enum CreateIndexTypeInput {
    String,
    Numeric,
    Date,
    Bool,
}

async fn create_index(
    State(search): State<App>,
    Path(name): Path<String>,
    Json(input): Json<CreateIndexInput>,
) -> Result<Json<()>, AppError> {
    search.create_index(&name, input)?;
    Ok(Json(()))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueryIndexInput {
    filter: Option<String>,
    sort: Option<SortInput>,
    page: Option<PageInput>,
    scope: Option<DeltaScopeInput>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SortInput {
    by: String,
    direction: SortDirectionInput,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
enum SortDirectionInput {
    Asc,
    Desc,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PageInput {
    start: Option<usize>,
    size: Option<usize>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryResponse {
    data: Vec<DataItemExternal>,
}

async fn query(
    State(search): State<App>,
    Path(name): Path<String>,
    Json(input): Json<QueryIndexInput>,
) -> Result<Json<QueryResponse>, AppError> {
    let data = search.query(&name, input)?;
    Ok(Json(QueryResponse { data }))
}
