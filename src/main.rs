use std::sync::{Arc, RwLock};

use axum::extract::{Path, State};
use axum::routing::{get, post, put};
use axum::{response::Json, Router};
use serde::{Deserialize, Serialize};

use delta_search::data::{DataItem, DataItemFieldsInput, DataItemId};
use delta_search::index::TypeDescriptor;
use delta_search::query::{
    FilterOption, FilterParser, OptionsQueryExecution, Pagination, QueryExecution,
};
use delta_search::storage::CreateFieldIndex;
use delta_search::Engine;

const DEFAULT_START_PAGE: usize = 0;
const DEFAULT_PAGE_SIZE: usize = 500;

#[derive(Clone)]
struct SearchEngine {
    inner: Arc<RwLock<Engine>>,
}

impl SearchEngine {
    fn init() -> SearchEngine {
        SearchEngine {
            inner: Arc::new(RwLock::new(Engine::init())),
        }
    }

    fn create_entity(&self, name: &str) {
        let mut engine = self.inner.write().unwrap();
        engine.create_entity(name.to_string());
    }

    fn add_items(&self, name: &str, items: Vec<DataItemInput>) {
        let mut engine = self.inner.write().unwrap();

        for input_item in items {
            let item = DataItem::new(input_item.id, input_item.fields.inner);
            engine.add(name, &item)
        }
    }

    fn query(&self, name: &str, input: QueryIndexInput) -> Vec<DataItem> {
        let engine = self.inner.read().unwrap();
        let execution = Self::build_query_execution(input);

        engine.query(name, execution)
    }

    fn build_query_execution(input: QueryIndexInput) -> QueryExecution {
        let mut execution = QueryExecution::new();

        if let Some(filter) = &input.filter {
            execution = execution.with_filter(FilterParser::parse_query(filter));
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

        execution = execution.with_pagination(pagination);

        execution
    }

    fn options(&self, name: &str) -> Vec<FilterOption> {
        let engine = self.inner.read().unwrap();
        engine.options(name, OptionsQueryExecution::new())
    }

    fn create_index(&self, name: &str, input: CreateIndexInput) {
        let mut engine = self.inner.write().unwrap();

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

        engine.create_index(name, command)
    }
}

#[tokio::main]
async fn main() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .unwrap();

    let search_engine = SearchEngine::init();

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

    axum::serve(listener, app).await.unwrap();
}

async fn create_entity(State(search): State<SearchEngine>, Path(name): Path<String>) -> Json<()> {
    search.create_entity(&name);
    Json(())
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
    State(search): State<SearchEngine>,
    Path(name): Path<String>,
    Json(input): Json<BulkUpsertEntity>,
) -> Json<()> {
    search.add_items(&name, input.data);
    Json(())
}

async fn get_options(
    State(search): State<SearchEngine>,
    Path(name): Path<String>,
) -> Json<Vec<FilterOption>> {
    let options = search.options(&name);
    Json(options)
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
    State(search): State<SearchEngine>,
    Path(name): Path<String>,
    Json(input): Json<CreateIndexInput>,
) -> Json<()> {
    search.create_index(&name, input);
    Json(())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueryIndexInput {
    filter: Option<String>,
    page: Option<PageInput>,
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
    State(search): State<SearchEngine>,
    Path(name): Path<String>,
    Json(input): Json<QueryIndexInput>,
) -> Json<QueryResponse> {
    let data = search.query(&name, input);
    Json(QueryResponse { data })
}
