use crate::index::{Indexable, IndexableValue};
use crate::query::{Delta, DeltaChange};
use crate::{DataItemId, EntityStorage, FieldValue};
use std::collections::HashSet;

pub fn create_random_players(count: usize) -> Vec<Player> {
    (0..count).map(create_player_from_index).collect()
}

pub fn create_player_from_index(index: usize) -> Player {
    Player {
        id: index,
        name: format!("Player {}", index),
        score: Some(index as f64),
        sport: if index % 2 == 0 {
            Sport::Basketball
        } else {
            Sport::Football
        },
        birth_date: "2000-01-01".to_string(),
    }
}

pub fn create_players_storage(data: Vec<Player>) -> EntityStorage<Player> {
    let mut storage = EntityStorage::new();

    storage.attach(data);
    storage.index();

    storage
}

#[derive(Debug, PartialEq, Clone)]
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

#[derive(Debug, PartialEq, Clone)]
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

pub(crate) struct DecreaseScoreDelta {
    id: DataItemId,
    score: f64,
}

impl DecreaseScoreDelta {
    pub(crate) fn new(id: DataItemId, score: f64) -> Self {
        DecreaseScoreDelta { id, score }
    }
}

impl Delta for DecreaseScoreDelta {
    type Value = Player;

    fn change(&self) -> DeltaChange {
        DeltaChange::new(self.id, "score".to_string())
            .before(FieldValue::numeric(self.score))
            .after(FieldValue::numeric(self.score - 1.0))
    }

    fn apply_data(&self, value: &mut Self::Value) {
        if let Some(score) = value.score.as_mut() {
            *score -= 1.0;
        }
    }
}

pub(crate) struct SwitchSportsDelta {
    id: DataItemId,
    current: Sport,
    new_sport: Sport,
}

impl SwitchSportsDelta {
    pub(crate) fn new(id: DataItemId, current: Sport, new_sport: Sport) -> Self {
        SwitchSportsDelta {
            id,
            current,
            new_sport,
        }
    }
}

impl Delta for SwitchSportsDelta {
    type Value = Player;

    fn change(&self) -> DeltaChange {
        DeltaChange::new(self.id, "sport".to_string())
            .before(FieldValue::string(self.current.as_string()))
            .after(FieldValue::string(self.new_sport.as_string()))
    }

    fn apply_data(&self, value: &mut Self::Value) {
        value.sport = self.new_sport.clone();
    }
}
