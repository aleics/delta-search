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
use tokio::sync::RwLock;

use delta_search::data::{DataItem, DataItemFieldsInput, DataItemId};
use delta_search::index::TypeDescriptor;
use delta_search::query::{
    FilterOption, FilterParser, OptionsQueryExecution, Pagination, QueryExecution, Sort,
    SortDirection,
};
use delta_search::storage::CreateFieldIndex;
use delta_search::Engine;

const DEFAULT_START_PAGE: usize = 0;
const DEFAULT_PAGE_SIZE: usize = 500;

#[derive(Clone)]
struct App {
    inner: Arc<RwLock<Engine>>,
}

impl App {
    fn init() -> Result<App, AppError> {
        let engine = Engine::init().map_err(|_| anyhow!("Could not initialize engine"))?;

        Ok(App {
            inner: Arc::new(RwLock::new(engine)),
        })
    }

    async fn create_entity(&self, name: &str) -> Result<(), AppError> {
        let mut engine = self.inner.write().await;
        engine
            .create_entity(name.to_string())
            .map_err(|_| anyhow!("Could not create entity `{}`", name))?;

        Ok(())
    }

    async fn add_items(&self, name: &str, items: Vec<DataItemInput>) -> Result<(), AppError> {
        let items: Vec<DataItem> = items
            .into_iter()
            .map(|input_item| DataItem::new(input_item.id, input_item.fields.inner))
            .collect();

        let engine = self.inner.read().await;
        engine
            .add_multiple(name, items.as_slice())
            .await
            .map_err(|_| anyhow!("Could not add items for entity `{}`", name))?;

        Ok(())
    }

    async fn query(&self, name: &str, input: QueryIndexInput) -> Result<Vec<DataItem>, AppError> {
        let execution = Self::build_query_execution(input)?;

        let engine = self.inner.read().await;
        engine
            .query(name, execution)
            .await
            .map_err(|_| anyhow!("Could not add items for entity `{}`", name).into())
    }

    fn build_query_execution(input: QueryIndexInput) -> Result<QueryExecution, AppError> {
        let mut execution = QueryExecution::new();

        if let Some(filter) = &input.filter {
            let parsed_filter =
                FilterParser::parse_query(filter).map_err(|_| AppError::InvalidFilterQuery)?;
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

        Ok(execution.with_pagination(pagination))
    }

    async fn options(&self, name: &str) -> Result<Vec<FilterOption>, AppError> {
        let engine = self.inner.read().await;
        engine
            .options(name, OptionsQueryExecution::new())
            .await
            .map_err(|_| anyhow!("Could not create options for entity `{}`", name).into())
    }

    async fn create_index(&self, name: &str, input: CreateIndexInput) -> Result<(), AppError> {
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

        let engine = self.inner.read().await;
        engine
            .create_index(name, command)
            .await
            .map_err(|_| anyhow!("Could not create index for entity `{}`", name).into())
    }
}

#[derive(Error, Debug)]
enum AppError {
    #[error("filter query is not valid")]
    InvalidFilterQuery,
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
            AppError::ServerError(err) => Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::new(format!("Something went wrong: {}", err)))
                .unwrap(),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), AppError> {
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();

    let search_engine = App::init()?;

    let app = Router::new()
        .route("/entities/:entity_name", post(create_entity))
        // Storage endpoints
        .route("/data/:entity_name", put(bulk_upsert_entity))
        // Index endpoints
        .route("/indices/:entity_name", put(create_index))
        // Search endpoints
        .route("/indices/:entity_name/options", get(get_options))
        .route("/indices/:entity_name/search", post(query))
        .with_state(search_engine);

    println!("delta-search is running...");

    axum::serve(listener, app).await.unwrap();

    Ok(())
}

async fn create_entity(
    State(search): State<App>,
    Path(name): Path<String>,
) -> Result<Json<()>, AppError> {
    search.create_entity(&name).await?;
    Ok(Json(()))
}

#[derive(Deserialize)]
struct BulkUpsertEntity {
    data: Vec<DataItemInput>,
}

#[derive(Deserialize)]
struct DataItemInput {
    id: DataItemId,
    fields: DataItemFieldsInput,
}

async fn bulk_upsert_entity(
    State(search): State<App>,
    Path(name): Path<String>,
    Json(input): Json<BulkUpsertEntity>,
) -> Result<Json<()>, AppError> {
    search.add_items(&name, input.data).await?;
    Ok(Json(()))
}

async fn get_options(
    State(search): State<App>,
    Path(name): Path<String>,
) -> Result<Json<Vec<FilterOption>>, AppError> {
    let options = search.options(&name).await?;
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
    search.create_index(&name, input).await?;
    Ok(Json(()))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueryIndexInput {
    filter: Option<String>,
    sort: Option<SortInput>,
    page: Option<PageInput>,
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
    data: Vec<DataItem>,
}

async fn query(
    State(search): State<App>,
    Path(name): Path<String>,
    Json(input): Json<QueryIndexInput>,
) -> Result<Json<QueryResponse>, AppError> {
    let data = search.query(&name, input).await?;
    Ok(Json(QueryResponse { data }))
}
