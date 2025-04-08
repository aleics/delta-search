use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::iter::FromIterator;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::data::FieldValue;
use crate::index::{StringTypeDescriptor, TypeDescriptor};
use crate::query::DeltaChange;
use crate::storage::{CreateFieldIndex, EntityStorage, StorageBuilder};
use crate::{DataItem, DataItemId, Engine};

pub fn michael_jordan() -> DataItem {
    Player::new(0, "Michael Jordan", Sport::Basketball, "1963-02-17", false)
        .with_score(10.0)
        .as_item()
}

pub fn lionel_messi() -> DataItem {
    Player::new(1, "Lionel Messi", Sport::Football, "1987-06-24", true)
        .with_score(9.0)
        .as_item()
}

pub fn cristiano_ronaldo() -> DataItem {
    Player::new(2, "Cristiano Ronaldo", Sport::Football, "1985-02-05", true)
        .with_score(9.0)
        .as_item()
}

pub fn roger() -> DataItem {
    Player::new(3, "Roger", Sport::Football, "1996-05-01", false)
        .with_score(5.0)
        .as_item()
}

pub fn david() -> DataItem {
    Player::new(4, "David", Sport::Basketball, "1974-10-01", false).as_item()
}

pub(crate) struct TestPlayerRunner {
    pub(crate) name: String,
    pub(crate) engine: Engine,
    pub(crate) path: String,
}

impl TestPlayerRunner {
    fn start(index: usize) -> Self {
        let name = format!("test_players_{}", index);
        let storage = StorageBuilder::new(&name).build().unwrap();
        let path = storage.get_path().as_os_str().to_str().unwrap().to_string();

        TestPlayerRunner {
            engine: Engine::with_entities(vec![storage]),
            name,
            path,
        }
    }
}

impl Drop for TestPlayerRunner {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.path).unwrap();
    }
}

pub(crate) struct TestRunners {
    runners: Mutex<Vec<TestPlayerRunner>>,
}

impl TestRunners {
    pub(crate) fn start(count: usize) -> Self {
        let runners = (0..count).map(TestPlayerRunner::start).collect();

        TestRunners {
            runners: Mutex::new(runners),
        }
    }

    pub(crate) fn start_runner(&self, items: Vec<DataItem>) -> TestPlayerRunner {
        let runner = self
            .runners
            .lock()
            .unwrap()
            .pop()
            .expect("No storages left - make sure you didn't exceed the test count");

        if let Some(entity) = runner.engine.entities.pin().get(&runner.name) {
            carry_players(items, entity);
        }

        runner
    }
}

pub fn create_random_players(count: u64) -> Vec<DataItem> {
    (0..count).map(create_player_from_index).collect()
}

pub fn create_player_from_index(index: u64) -> DataItem {
    let base = if index % 2 == 0 {
        10.0
    } else {
        2.0 * index as f64
    };

    Player {
        id: index,
        name: format!("Player {}", base),
        score: Some(base),
        sport: if index % 2 == 0 {
            Sport::Basketball
        } else {
            Sport::Football
        },
        birth_date: "2000-01-01".to_string(),
        active: true,
    }
    .as_item()
}

pub fn create_players_storage(name: &str, data: Vec<DataItem>) -> EntityStorage {
    let storage = StorageBuilder::new(name).build().unwrap();
    carry_players(data, &storage);

    storage
}

fn carry_players(items: Vec<DataItem>, storage: &EntityStorage) {
    storage.carry(&items).unwrap();

    storage
        .create_indices(vec![
            CreateFieldIndex {
                name: "name".to_string(),
                descriptor: TypeDescriptor::String(StringTypeDescriptor { term: true }),
            },
            CreateFieldIndex {
                name: "sport".to_string(),
                descriptor: TypeDescriptor::Enum(HashSet::from_iter([
                    Sport::Basketball.as_string(),
                    Sport::Football.as_string(),
                ])),
            },
            CreateFieldIndex {
                name: "birth_date".to_string(),
                descriptor: TypeDescriptor::Date,
            },
            CreateFieldIndex {
                name: "score".to_string(),
                descriptor: TypeDescriptor::Numeric,
            },
            CreateFieldIndex {
                name: "active".to_string(),
                descriptor: TypeDescriptor::Bool,
            },
        ])
        .unwrap();
}

pub fn decrease_score_deltas(data: &[DataItem], size: usize) -> Vec<DeltaChange> {
    let mut deltas = Vec::new();

    for item in data.iter().take(size) {
        if let Some(score) = item
            .fields
            .get("score")
            .and_then(|field| field.as_decimal())
            .copied()
        {
            deltas.push(DecreaseScoreDelta::create(item.id, score.into()));
        }
    }

    deltas
}

pub fn switch_sports_deltas(data: &[DataItem], size: usize) -> Vec<DeltaChange> {
    let mut deltas = Vec::new();

    for item in data.iter().take(size) {
        let previous = item
            .fields
            .get("sport")
            .and_then(|field| field.as_string())
            .and_then(|sport| Sport::try_from_string(sport))
            .unwrap();

        let new_sport = match previous {
            Sport::Basketball => Sport::Football,
            Sport::Football => Sport::Basketball,
        };

        deltas.push(SwitchSportsDelta::create(item.id, new_sport));
    }

    deltas
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum Sport {
    Basketball,
    Football,
}

impl Sport {
    pub fn as_string(&self) -> String {
        match self {
            Sport::Basketball => "Basketball".to_string(),
            Sport::Football => "Football".to_string(),
        }
    }

    pub fn try_from_string(string: &str) -> Option<Sport> {
        match string {
            "Basketball" => Some(Sport::Basketball),
            "Football" => Some(Sport::Football),
            _ => None,
        }
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Player {
    pub id: u64,
    pub name: String,
    pub score: Option<f64>,
    pub sport: Sport,
    pub birth_date: String,
    pub active: bool,
}

impl Player {
    pub fn new(id: u64, name: &str, sport: Sport, birth_date: &str, active: bool) -> Self {
        Player {
            id,
            name: name.to_string(),
            score: None,
            sport,
            birth_date: birth_date.to_string(),
            active,
        }
    }

    pub fn with_score(mut self, score: f64) -> Self {
        self.score = Some(score);
        self
    }

    pub fn as_item(&self) -> DataItem {
        let mut fields = BTreeMap::new();
        fields.insert("name".to_string(), FieldValue::str(&self.name));
        fields.insert(
            "sport".to_string(),
            FieldValue::str(&self.sport.as_string()),
        );
        fields.insert("birth_date".to_string(), FieldValue::str(&self.birth_date));
        fields.insert("active".to_string(), FieldValue::Bool(self.active));

        if let Some(score) = self.score {
            fields.insert("score".to_string(), FieldValue::dec(score));
        }

        DataItem::new(self.id, fields)
    }
}

pub struct DecreaseScoreDelta;

impl DecreaseScoreDelta {
    pub fn create(id: DataItemId, score: f64) -> DeltaChange {
        DeltaChange::new(id, "score".to_string(), FieldValue::dec(score - 1.0))
    }
}

pub struct SwitchSportsDelta;

impl SwitchSportsDelta {
    pub fn create(id: DataItemId, after: Sport) -> DeltaChange {
        DeltaChange::new(
            id,
            "sport".to_string(),
            FieldValue::String(after.as_string()),
        )
    }
}
