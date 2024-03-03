use crate::index::{Indexable, IndexableValue};
use crate::query::{Delta, DeltaChange};
use crate::{DataItemId, FieldValue};
use std::collections::HashSet;

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
    pub score: f64,
    pub sport: Sport,
    pub birth_date: String,
}

impl Indexable for Player {
    fn id(&self) -> DataItemId {
        self.id
    }

    fn index_values(&self) -> Vec<IndexableValue> {
        vec![
            IndexableValue::string("name".to_string(), self.name.to_string()),
            IndexableValue::numeric("score".to_string(), self.score),
            IndexableValue::enumerate(
                "sport".to_string(),
                self.sport.as_string(),
                HashSet::from_iter([Sport::Basketball.as_string(), Sport::Football.as_string()]),
            ),
            IndexableValue::date_iso("birth_date".to_string(), &self.birth_date),
        ]
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
        value.score -= 1.0;
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
