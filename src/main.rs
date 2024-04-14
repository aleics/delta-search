use std::sync::{Arc, RwLock};

use axum::{response::Json, Router};
use axum::extract::State;
use axum::routing::{get, put};
use serde::Deserialize;

use delta_search::data::{DataItem, DataItemFieldsInput, DataItemId};
use delta_search::Engine;
use delta_search::index::TypeDescriptor;
use delta_search::query::{FilterOption, OptionsQueryExecution};
use delta_search::storage::{CreateFieldIndex, StorageBuilder};

#[derive(Clone)]
struct SearchEngine {
    inner: Arc<RwLock<Engine>>,
}

impl SearchEngine {
    fn init() -> SearchEngine {
        let storage = StorageBuilder::new("players_prod").build();

        SearchEngine {
            inner: Arc::new(RwLock::new(Engine::new(storage))),
        }
    }

    fn add_items(&self, items: Vec<DataItemInput>) {
        let mut engine = self.inner.write().unwrap();

        for input_item in items {
            let item = DataItem::new(input_item.id, input_item.fields.inner);
            engine.add(&item)
        }
    }

    fn options(&self) -> Vec<FilterOption> {
        let engine = self.inner.read().unwrap();
        engine.options(OptionsQueryExecution::new())
    }

    fn create_index(&self, input: CreateIndexInput) {
        let mut engine = self.inner.write().unwrap();

        let descriptor = match input.kind {
            CreateIndexTypeInput::String => TypeDescriptor::String,
            CreateIndexTypeInput::Numeric => TypeDescriptor::Numeric,
            CreateIndexTypeInput::Date => TypeDescriptor::Date,
            CreateIndexTypeInput::Bool => TypeDescriptor::Bool,
        };

        engine.create_index(CreateFieldIndex {
            name: input.name,
            descriptor,
        })
    }
}

#[tokio::main]
async fn main() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .unwrap();

    let search_engine = SearchEngine::init();

    let app = Router::new()
        // Storage endpoints
        .route("/entities", put(upsert_entity))
        // Search endpoints
        .route("/options", get(get_options))
        // Index endpoints
        .route("/indices", put(create_index))
        .with_state(search_engine);

    axum::serve(listener, app).await.unwrap();
}

#[derive(Deserialize)]
struct UpsertEntityInput {
    data: Vec<DataItemInput>,
}

#[derive(Deserialize)]
struct DataItemInput {
    id: DataItemId,
    fields: DataItemFieldsInput,
}

async fn upsert_entity(
    State(search): State<SearchEngine>,
    Json(input): Json<UpsertEntityInput>,
) -> Json<()> {
    search.add_items(input.data);
    Json(())
}

async fn get_options(State(search): State<SearchEngine>) -> Json<Vec<FilterOption>> {
    let options = search.options();
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
    Json(input): Json<CreateIndexInput>,
) -> Json<()> {
    search.create_index(input);
    Json(())
}
