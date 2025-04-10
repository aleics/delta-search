use std::sync::Arc;

use anyhow::anyhow;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{post, put};
use axum::{response::Json, Router};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::format_description::well_known::Iso8601;
use time::Date;

use delta_search::data::{
    DataItem, DataItemFieldsExternal, DataItemId, FieldValue, FieldValueExternal,
};
use delta_search::index::{StringTypeDescriptor, TypeDescriptor};
use delta_search::query::{
    DeltaChange, DeltaScope, FilterOption, OptionsQueryExecution, QueryExecution,
};
use delta_search::storage::CreateFieldIndex;
use delta_search::{Engine, EngineError};
use tracing::{error, info};

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
            .map_err(|err| match err {
                EngineError::EntityAlreadyExists { name } => AppError::EntityAlreadyExists {
                    message: format!("Entity with name \"{}\" already exists", name),
                },
                _ => AppError::ServerError(anyhow!("Could not create entity `{}`", name)),
            })?;

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
            .store_deltas(name, &scope, deltas)
            .inspect_err(|err| error!("Could not store deltas: {}", err))
            .map_err(|_| anyhow!("Could not store deltas for entity `{}`", name))?;

        Ok(())
    }

    fn query(&self, input: QueryInput) -> Result<Vec<DataItemExternal>, AppError> {
        let execution = Self::build_query_execution(input)?;

        self.inner
            .query(execution)
            .map(|items| items.into_iter().map(DataItemExternal::from_item).collect())
            .inspect_err(|err| error!("Query could not be executed: {}", err))
            .map_err(|_| anyhow!("Query could not be executed").into())
    }

    fn build_query_execution(input: QueryInput) -> Result<QueryExecution, AppError> {
        QueryExecution::parse_query(&input.query).map_err(|_| AppError::InvalidFilterQuery)
    }

    fn options(&self, input: QueryOptionsInput) -> Result<Vec<FilterOption>, AppError> {
        let execution = Self::build_options_execution(input)?;

        self.inner
            .options(execution)
            .inspect_err(|err| error!("Could not create options: {}", err))
            .map_err(|_| anyhow!("Could not create options").into())
    }

    fn build_options_execution(
        input: QueryOptionsInput,
    ) -> Result<OptionsQueryExecution, AppError> {
        OptionsQueryExecution::parse_query(&input.query).map_err(|_| AppError::InvalidFilterQuery)
    }

    fn create_index(&self, name: &str, input: CreateIndexInput) -> Result<(), AppError> {
        let command = match input {
            CreateIndexInput::String(create_index) => CreateFieldIndex {
                name: create_index.name,
                descriptor: TypeDescriptor::String(StringTypeDescriptor {
                    term: create_index.term,
                }),
            },
            CreateIndexInput::Numeric(create_index) => CreateFieldIndex {
                name: create_index.name,
                descriptor: TypeDescriptor::Numeric,
            },
            CreateIndexInput::Date(create_index) => CreateFieldIndex {
                name: create_index.name,
                descriptor: TypeDescriptor::Date,
            },
            CreateIndexInput::Bool(create_index) => CreateFieldIndex {
                name: create_index.name,
                descriptor: TypeDescriptor::Bool,
            },
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
    #[error("entity already exists")]
    EntityAlreadyExists { message: String },
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
            AppError::EntityAlreadyExists { message } => Response::builder()
                .status(StatusCode::CONFLICT)
                .body(Body::new(format!("Conflict: \"{}\"", message)))
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
        .route("/options", post(options))
        .route("/search", post(query))
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

#[derive(Debug, Serialize, Deserialize)]
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
struct BulkStoreDeltasInput {
    scope: DeltaScopeInput,
    deltas: Vec<DeltaChangeInput>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeltaChangeInput {
    id: DataItemId,
    field_name: String,
    after: FieldValueExternal,
}

impl DeltaChangeInput {
    fn map_delta_change(self) -> Result<DeltaChange, AppError> {
        let after = DeltaChangeInput::map_field(&self.field_name, self.after)?;

        Ok(DeltaChange {
            id: self.id,
            field_name: self.field_name,
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeltaScopeInput {
    branch: Option<u32>,
    date: String,
}

impl DeltaScopeInput {
    fn map_delta_scope(self) -> Result<DeltaScope, AppError> {
        let date = parse_date(&self.date).map_err(|_| AppError::InvalidRequest {
            message: "Date format is invalid. Only ISO 8601 is supported.".to_string(),
        })?;

        Ok(DeltaScope::new(self.branch, date))
    }
}

async fn bulk_add_deltas(
    State(search): State<App>,
    Path(name): Path<String>,
    Json(input): Json<BulkStoreDeltasInput>,
) -> Result<Json<()>, AppError> {
    search.add_deltas(&name, input.scope, input.deltas)?;
    Ok(Json(()))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueryOptionsInput {
    query: String,
}

async fn options(
    State(search): State<App>,
    Json(input): Json<QueryOptionsInput>,
) -> Result<Json<Vec<FilterOption>>, AppError> {
    let options = search.options(input)?;
    Ok(Json(options))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(tag = "type")]
enum CreateIndexInput {
    String(CreateStringIndexTypeInput),
    Numeric(CreateNumericIndexTypeInput),
    Date(CreateDateIndexTypeInput),
    Bool(CreateBoolIndexTypeInput),
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateStringIndexTypeInput {
    name: String,
    term: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateNumericIndexTypeInput {
    name: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateDateIndexTypeInput {
    name: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateBoolIndexTypeInput {
    name: String,
}

async fn create_index(
    State(search): State<App>,
    Path(name): Path<String>,
    Json(input): Json<CreateIndexInput>,
) -> Result<Json<()>, AppError> {
    search.create_index(&name, input)?;
    Ok(Json(()))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueryInput {
    query: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryResponse {
    data: Vec<DataItemExternal>,
}

async fn query(
    State(search): State<App>,
    Json(input): Json<QueryInput>,
) -> Result<Json<QueryResponse>, AppError> {
    let data = search.query(input)?;
    Ok(Json(QueryResponse { data }))
}
