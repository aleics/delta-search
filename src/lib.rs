pub mod index;
pub mod query;

use bimap::BiHashMap;
use ordered_float::OrderedFloat;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};

use crate::index::Indexable;
use index::Index;
use query::QueryExecution;
use time::{Date, OffsetDateTime, Time};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldValue {
    String(String),
    Numeric(OrderedFloat<f64>),
    Date(OffsetDateTime),
}

impl FieldValue {
    pub fn string(value: String) -> FieldValue {
        FieldValue::String(value)
    }

    pub fn numeric(value: f64) -> FieldValue {
        FieldValue::Numeric(OrderedFloat(value))
    }

    pub fn date(date: Date) -> FieldValue {
        FieldValue::Date(OffsetDateTime::new_utc(date, Time::MIDNIGHT))
    }

    fn as_numeric(&self) -> Option<&OrderedFloat<f64>> {
        if let FieldValue::Numeric(value) = self {
            Some(value)
        } else {
            None
        }
    }

    fn get_numeric(self) -> Option<OrderedFloat<f64>> {
        if let FieldValue::Numeric(value) = self {
            Some(value)
        } else {
            None
        }
    }

    fn as_string(&self) -> Option<&String> {
        if let FieldValue::String(value) = self {
            Some(value)
        } else {
            None
        }
    }

    fn get_string(self) -> Option<String> {
        if let FieldValue::String(value) = self {
            Some(value)
        } else {
            None
        }
    }

    fn get_date_epoch(&self) -> Option<i64> {
        if let FieldValue::Date(value) = self {
            Some(value.unix_timestamp())
        } else {
            None
        }
    }
}

impl Display for FieldValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FieldValue::String(value) => write!(f, "{}", value),
            FieldValue::Numeric(value) => write!(f, "{}", value.0),
            FieldValue::Date(value) => write!(f, "{}", value),
        }
    }
}

pub type DataItemId = usize;

pub struct EntityStorage<T> {
    /// Indices available associated by data's field name
    indices: HashMap<String, Index>,

    /// Mapping between position of a data item in the index and its ID
    position_id: BiHashMap<u32, DataItemId>,

    /// Data available in the storage associated by the ID
    data: HashMap<DataItemId, T>,
}

impl<T: Indexable> EntityStorage<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn attach<I: IntoIterator<Item = T>>(&mut self, data: I) {
        for item in data {
            self.data.insert(item.id(), item);
        }
    }

    pub fn index(&mut self) {
        for (position, (id, item)) in self.data.iter().enumerate() {
            let position = position as u32;

            for property in item.index_values() {
                // Create index for the key value
                let index = self
                    .indices
                    .entry(property.name)
                    .or_insert(Index::from_type(&property.descriptor));

                index.put(property.value, position);
            }

            // Associate index position to the field ID
            self.position_id.insert(position, *id);
        }
    }

    fn get_id_by_position(&self, position: &u32) -> Option<&DataItemId> {
        self.position_id.get_by_left(position)
    }

    fn get_position_by_id(&self, id: &DataItemId) -> Option<&u32> {
        self.position_id.get_by_right(id)
    }
}

impl<T> Default for EntityStorage<T> {
    fn default() -> Self {
        EntityStorage {
            indices: Default::default(),
            position_id: Default::default(),
            data: Default::default(),
        }
    }
}

pub struct Engine<T> {
    storage: EntityStorage<T>,
}

impl<T> Engine<T> {
    pub fn new(storage: EntityStorage<T>) -> Self {
        Engine { storage }
    }
}

impl<T: Indexable + Clone> Engine<T> {
    pub fn query(&self, execution: QueryExecution<T>) -> Vec<T> {
        execution.run(&self.storage)
    }
}

#[cfg(test)]
mod tests {
    use crate::index::IndexableValue;
    use crate::query::{CompositeFilter, Delta, DeltaChange, Pagination, QueryExecution, Sort};
    use crate::{DataItemId, Engine, EntityStorage, FieldValue, Indexable};
    use lazy_static::lazy_static;
    use std::collections::HashSet;
    use time::{Date, Month};

    #[derive(Debug, PartialEq, Clone)]
    enum Sport {
        Basketball,
        Football,
    }

    impl Sport {
        fn as_string(&self) -> String {
            match self {
                Sport::Basketball => "basketball".to_string(),
                Sport::Football => "football".to_string(),
            }
        }
    }

    #[derive(Debug, PartialEq, Clone)]
    struct Player {
        id: usize,
        name: String,
        score: f64,
        sport: Sport,
        birth_date: String,
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
                    HashSet::from_iter([
                        Sport::Basketball.as_string(),
                        Sport::Football.as_string(),
                    ]),
                ),
                IndexableValue::date_iso("birth_date".to_string(), &self.birth_date),
            ]
        }
    }

    struct DecreaseScoreDelta {
        id: DataItemId,
        score: f64,
    }

    impl DecreaseScoreDelta {
        fn new(id: DataItemId, score: f64) -> Self {
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

    struct SwitchSportsDelta {
        id: DataItemId,
        current: Sport,
        new_sport: Sport,
    }

    impl SwitchSportsDelta {
        fn new(id: DataItemId, current: Sport, new_sport: Sport) -> Self {
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

    lazy_static! {
        static ref MICHAEL_JORDAN: Player = Player {
            id: 0,
            name: "Michael Jordan".to_string(),
            score: 10.0,
            sport: Sport::Basketball,
            birth_date: "1963-02-17".to_string()
        };
        static ref LIONEL_MESSI: Player = Player {
            id: 1,
            name: "Lionel Messi".to_string(),
            score: 9.0,
            sport: Sport::Football,
            birth_date: "1987-06-24".to_string()
        };
        static ref CRISTIANO_RONALDO: Player = Player {
            id: 2,
            name: "Cristiano Ronaldo".to_string(),
            score: 9.0,
            sport: Sport::Football,
            birth_date: "1985-02-05".to_string()
        };
        static ref ROGER: Player = Player {
            id: 3,
            name: "Roger".to_string(),
            score: 5.0,
            sport: Sport::Football,
            birth_date: "1996-05-01".to_string()
        };
    }

    fn create_random_players(count: usize) -> Vec<Player> {
        (0..count).map(create_player_from_index).collect()
    }

    fn create_player_from_index(index: usize) -> Player {
        Player {
            id: index,
            name: format!("Player {}", index),
            score: index as f64,
            sport: if index % 2 == 0 {
                Sport::Basketball
            } else {
                Sport::Football
            },
            birth_date: "2000-01-01".to_string(),
        }
    }

    fn storage(data: Vec<Player>) -> EntityStorage<Player> {
        let mut storage = EntityStorage::new();

        storage.attach(data);
        storage.index();

        storage
    }

    #[test]
    fn applies_enum_eq_filter() {
        // given
        let storage = storage(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            CRISTIANO_RONALDO.clone(),
        ]);
        let engine = Engine::new(storage);

        // when
        let execution = QueryExecution::new(CompositeFilter::eq(
            "sport",
            FieldValue::string("football".to_string()),
        ));
        let mut matches = engine.query(execution);

        // then
        matches.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(
            matches,
            vec![LIONEL_MESSI.clone(), CRISTIANO_RONALDO.clone()]
        );
    }

    #[test]
    fn applies_date_ge_filter() {
        // given
        let storage = storage(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            CRISTIANO_RONALDO.clone(),
            ROGER.clone(),
        ]);
        let engine = Engine::new(storage);

        // when
        let execution = QueryExecution::new(CompositeFilter::ge(
            "birth_date",
            FieldValue::date(Date::from_calendar_date(1990, Month::January, 1).unwrap()),
        ));
        let mut matches = engine.query(execution);

        // then
        matches.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(matches, vec![ROGER.clone()]);
    }

    #[test]
    fn applies_date_between_filter() {
        // given
        let storage = storage(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            CRISTIANO_RONALDO.clone(),
            ROGER.clone(),
        ]);
        let engine = Engine::new(storage);

        // when
        let execution = QueryExecution::new(CompositeFilter::between(
            "birth_date",
            FieldValue::date(Date::from_calendar_date(1970, Month::January, 1).unwrap()),
            FieldValue::date(Date::from_calendar_date(1990, Month::January, 1).unwrap()),
        ));
        let mut matches = engine.query(execution);

        // then
        matches.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(
            matches,
            vec![LIONEL_MESSI.clone(), CRISTIANO_RONALDO.clone()]
        );
    }

    #[test]
    fn applies_numeric_between_filter() {
        // given
        let storage = storage(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            ROGER.clone(),
        ]);
        let engine = Engine::new(storage);

        // when
        let execution = QueryExecution::new(CompositeFilter::between(
            "score",
            FieldValue::numeric(6.0),
            FieldValue::numeric(10.0),
        ));
        let mut matches = engine.query(execution);

        // then
        matches.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(matches, vec![MICHAEL_JORDAN.clone(), LIONEL_MESSI.clone()]);
    }

    #[test]
    fn applies_numeric_ge_filter() {
        // given
        let storage = storage(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            ROGER.clone(),
        ]);
        let engine = Engine::new(storage);

        // when
        let execution = QueryExecution::new(CompositeFilter::ge("score", FieldValue::numeric(6.0)));
        let mut matches = engine.query(execution);

        // then
        matches.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(matches, vec![MICHAEL_JORDAN.clone(), LIONEL_MESSI.clone()]);
    }

    #[test]
    fn applies_numeric_le_filter() {
        // given
        let storage = storage(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            ROGER.clone(),
        ]);
        let engine = Engine::new(storage);

        // when
        let execution = QueryExecution::new(CompositeFilter::le("score", FieldValue::numeric(6.0)));
        let mut matches = engine.query(execution);

        // then
        matches.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(matches, vec![ROGER.clone()]);
    }

    #[test]
    fn applies_numeric_delta() {
        // given
        let storage = storage(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            CRISTIANO_RONALDO.clone(),
        ]);
        let deltas = vec![
            DecreaseScoreDelta::new(MICHAEL_JORDAN.id, MICHAEL_JORDAN.score),
            DecreaseScoreDelta::new(LIONEL_MESSI.id, LIONEL_MESSI.score),
        ];

        let engine = Engine::new(storage);

        // when
        let execution = QueryExecution::new(CompositeFilter::eq(
            "sport",
            FieldValue::string("football".to_string()),
        ))
        .with_deltas(deltas);

        let mut matches = engine.query(execution);

        // then
        matches.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(
            matches,
            vec![
                Player {
                    id: LIONEL_MESSI.id,
                    name: LIONEL_MESSI.name.to_string(),
                    score: 8.0,
                    sport: LIONEL_MESSI.sport.clone(),
                    birth_date: LIONEL_MESSI.birth_date.clone()
                },
                CRISTIANO_RONALDO.clone()
            ]
        );
    }

    #[test]
    fn applies_enum_delta() {
        // given
        let storage = storage(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            CRISTIANO_RONALDO.clone(),
        ]);
        let deltas = vec![SwitchSportsDelta::new(
            MICHAEL_JORDAN.id,
            MICHAEL_JORDAN.sport.clone(),
            Sport::Football,
        )];

        let engine = Engine::new(storage);

        // when
        let execution = QueryExecution::new(CompositeFilter::eq(
            "sport",
            FieldValue::string("football".to_string()),
        ))
        .with_deltas(deltas);

        let mut matches = engine.query(execution);

        // then
        matches.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(
            matches,
            vec![
                Player {
                    id: MICHAEL_JORDAN.id,
                    name: MICHAEL_JORDAN.name.to_string(),
                    score: MICHAEL_JORDAN.score,
                    sport: Sport::Football,
                    birth_date: MICHAEL_JORDAN.birth_date.clone()
                },
                LIONEL_MESSI.clone(),
                CRISTIANO_RONALDO.clone()
            ]
        );
    }

    #[test]
    fn applies_pagination() {
        // given
        let storage = storage(create_random_players(20));
        let engine = Engine::new(storage);

        let filter = CompositeFilter::eq("sport", FieldValue::string("football".to_string()));
        let pagination = Pagination::new(2, 5);

        // when
        let execution = QueryExecution::new(filter)
            .with_sort(Sort::new("score".to_string()))
            .with_pagination(pagination);

        let mut matches = engine.query(execution);

        // then
        matches.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(
            matches,
            vec![
                create_player_from_index(5),
                create_player_from_index(7),
                create_player_from_index(9),
                create_player_from_index(11),
                create_player_from_index(13)
            ]
        );
    }
}
