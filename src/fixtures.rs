use std::collections::HashSet;
use std::fs;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::index::{Indexable, IndexableValue};
use crate::query::{Delta, DeltaChange};
use crate::storage::{EntityStorage, StorageBuilder};
use crate::{DataItemId, Engine, FieldValue};

pub(crate) struct TestPlayerRunner {
    pub(crate) engine: Engine<Player>,
}

impl TestPlayerRunner {
    fn start(index: usize) -> Self {
        let name = format!("test_players_{}", index);
        let storage = StorageBuilder::disk(&name).build();

        TestPlayerRunner {
            engine: Engine::new(storage),
        }
    }

    fn clean_up(&self) {
        let path = self.engine.storage.get_path();
        fs::remove_dir_all(path).unwrap();
    }
}

impl Drop for TestPlayerRunner {
    fn drop(&mut self) {
        self.clean_up()
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

    pub(crate) fn start_runner(&self, players: Vec<Player>) -> TestPlayerRunner {
        let mut runner = self
            .runners
            .lock()
            .unwrap()
            .pop()
            .expect("No storages left - make sure you didn't exceed the test count");

        runner.engine.storage.carry(players);

        runner
    }
}

pub fn create_random_players(count: usize) -> Vec<Player> {
    (0..count).map(create_player_from_index).collect()
}

pub fn create_player_from_index(index: usize) -> Player {
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
    }
}

pub fn create_players_in_memory_storage(data: Vec<Player>) -> EntityStorage<Player> {
    let mut storage = StorageBuilder::in_memory().build();
    storage.carry(data);

    storage
}

pub fn create_players_disk_storage(name: &str, data: Vec<Player>) -> EntityStorage<Player> {
    let mut storage = StorageBuilder::disk(name).build();
    storage.carry(data);

    storage
}

pub fn decrease_score_deltas(data: &[Player], size: usize) -> Vec<DecreaseScoreDelta> {
    let mut deltas = Vec::new();

    for player in data.iter().take(size) {
        if let Some(score) = player.score {
            deltas.push(DecreaseScoreDelta::new(player.id, score));
        }
    }

    deltas
}

pub fn switch_sports_deltas(data: &[Player], size: usize) -> Vec<SwitchSportsDelta> {
    let mut deltas = Vec::new();

    for player in data.iter().take(size) {
        let new_sport = match player.sport {
            Sport::Basketball => Sport::Football,
            Sport::Football => Sport::Basketball,
        };
        deltas.push(SwitchSportsDelta::new(
            player.id,
            player.sport.clone(),
            new_sport,
        ));
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
            Sport::Basketball => "basketball".to_string(),
            Sport::Football => "football".to_string(),
        }
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Player {
    pub id: usize,
    pub name: String,
    pub score: Option<f64>,
    pub sport: Sport,
    pub birth_date: String,
}

impl Player {
    pub fn new(id: usize, name: &str, sport: Sport, birth_date: &str) -> Self {
        Player {
            id,
            name: name.to_string(),
            score: None,
            sport,
            birth_date: birth_date.to_string(),
        }
    }

    pub fn with_score(mut self, score: f64) -> Self {
        self.score = Some(score);
        self
    }
}

impl Indexable for Player {
    fn id(&self) -> DataItemId {
        self.id
    }

    fn index_values(&self) -> Vec<IndexableValue> {
        let mut values = vec![
            IndexableValue::string("name".to_string(), self.name.to_string()),
            IndexableValue::enumerate(
                "sport".to_string(),
                self.sport.as_string(),
                HashSet::from_iter([Sport::Basketball.as_string(), Sport::Football.as_string()]),
            ),
            IndexableValue::date_iso("birth_date".to_string(), &self.birth_date),
        ];

        if let Some(score) = &self.score {
            values.push(IndexableValue::numeric("score".to_string(), *score));
        }

        values
    }
}

#[derive(Clone)]
pub struct DecreaseScoreDelta {
    id: DataItemId,
    before: f64,
    after: f64,
}

impl DecreaseScoreDelta {
    pub(crate) fn new(id: DataItemId, score: f64) -> Self {
        DecreaseScoreDelta {
            id,
            before: score,
            after: score - 1.0,
        }
    }
}

impl Delta for DecreaseScoreDelta {
    type Value = Player;

    fn change(&self) -> DeltaChange {
        DeltaChange::new(self.id, "score".to_string())
            .before(FieldValue::numeric(self.before))
            .after(FieldValue::numeric(self.after))
    }

    // TODO: if the DB should be accessible via REST API, this would not work
    // TODO: we could serialise the provided data as a simple key-value map, and change the values on the map
    fn apply_data(&self, value: &mut Self::Value) {
        if let Some(score) = value.score.as_mut() {
            *score = self.after;
        }
    }
}

#[derive(Clone)]
pub struct SwitchSportsDelta {
    id: DataItemId,
    before: Sport,
    after: Sport,
}

impl SwitchSportsDelta {
    pub fn new(id: DataItemId, before: Sport, after: Sport) -> Self {
        SwitchSportsDelta { id, before, after }
    }
}

impl Delta for SwitchSportsDelta {
    type Value = Player;

    fn change(&self) -> DeltaChange {
        DeltaChange::new(self.id, "sport".to_string())
            .before(FieldValue::string(self.before.as_string()))
            .after(FieldValue::string(self.after.as_string()))
    }

    fn apply_data(&self, value: &mut Self::Value) {
        value.sport = self.after.clone();
    }
}
