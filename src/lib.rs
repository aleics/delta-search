use papaya::HashMap;
use std::slice;

use thiserror::Error;

use query::{DeltaScope, QueryError};
use storage::StorageError;

use crate::data::{DataItem, DataItemId};
use crate::query::{DeltaChange, FilterOption, OptionsQueryExecution, QueryExecution};
use crate::storage::{CreateFieldIndex, EntityStorage, StorageBuilder};

pub mod data;
#[cfg(feature = "test-fixtures")]
pub mod fixtures;
pub mod index;
pub mod query;
pub mod storage;

pub struct Engine {
    entities: HashMap<String, EntityStorage>,
}

impl Engine {
    pub fn init() -> Result<Self, EngineError> {
        let entities = HashMap::new();

        for name in storage::read_stored_entity_names() {
            let storage = StorageBuilder::new(&name).build()?;
            entities.pin().insert(name, storage);
        }

        Ok(Engine { entities })
    }

    pub fn with_entities(entries: Vec<EntityStorage>) -> Self {
        let entities = HashMap::new();
        for entry in entries {
            entities.pin().insert(entry.id.clone(), entry);
        }
        Engine { entities }
    }

    pub fn create_entity(&self, name: String) -> Result<(), EngineError> {
        if self.entities.pin().contains_key(&name) {
            return Err(EngineError::EntityAlreadyExists { name });
        }

        let entity = StorageBuilder::new(&name).build()?;
        self.entities.pin().insert(name, entity);

        Ok(())
    }

    pub fn query(&self, execution: QueryExecution) -> Result<Vec<DataItem>, EngineError> {
        let items = if let Some(entity) = self.entities.pin().get(&execution.entity) {
            execution.run(entity)?
        } else {
            Vec::new()
        };

        Ok(items)
    }

    pub fn options(
        &self,
        execution: OptionsQueryExecution,
    ) -> Result<Vec<FilterOption>, EngineError> {
        let options = if let Some(entity) = self.entities.pin().get(&execution.entity) {
            execution.run(entity)?
        } else {
            Vec::new()
        };
        Ok(options)
    }

    pub fn add(&self, name: &str, item: &DataItem) -> Result<(), EngineError> {
        self.add_multiple(name, slice::from_ref(item))
    }

    pub fn add_multiple(&self, name: &str, items: &[DataItem]) -> Result<(), EngineError> {
        if let Some(entity) = self.entities.pin().get(name) {
            entity.add(items)?;
        }
        Ok(())
    }

    pub fn remove(&self, name: &str, id: &DataItemId) -> Result<(), EngineError> {
        if let Some(entity) = self.entities.pin().get(name) {
            entity.remove(slice::from_ref(id))?;
        }
        Ok(())
    }

    pub fn store_deltas(
        &self,
        name: &str,
        scope: &DeltaScope,
        deltas: Vec<DeltaChange>,
    ) -> Result<(), EngineError> {
        if let Some(entity) = self.entities.pin().get(name) {
            entity.add_deltas(scope, deltas)?;
        }
        Ok(())
    }

    pub fn clear(&self, name: &str) -> Result<(), EngineError> {
        if let Some(entity) = self.entities.pin().get(name) {
            entity.clear()?;
        }
        Ok(())
    }

    pub fn create_index(&self, name: &str, command: CreateFieldIndex) -> Result<(), EngineError> {
        if let Some(entity) = self.entities.pin().get(name) {
            entity.create_indices(vec![command])?;

            Ok(())
        } else {
            Err(EngineError::EntityNotFound)
        }
    }
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum EngineError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Query(#[from] QueryError),
    #[error("entity not found")]
    EntityNotFound,
    #[error("entity already exists")]
    EntityAlreadyExists { name: String },
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::iter::FromIterator;

    use lazy_static::lazy_static;
    use time::{Date, Month};

    use crate::data::{DataItem, FieldValue};
    use crate::fixtures::{
        create_player_from_index, create_random_players, cristiano_ronaldo, david, lionel_messi,
        michael_jordan, roger, DecreaseScoreDelta, Player, Sport, SwitchSportsDelta, TestRunners,
    };
    use crate::query::{
        CompositeFilter, DeltaScope, FilterOption, OptionsQueryExecution, Pagination,
        QueryExecution, Sort, SortDirection,
    };

    lazy_static! {
        static ref STORAGES: TestRunners = TestRunners::start(24);
        static ref MICHAEL_JORDAN: DataItem = michael_jordan();
        static ref LIONEL_MESSI: DataItem = lionel_messi();
        static ref CRISTIANO_RONALDO: DataItem = cristiano_ronaldo();
        static ref ROGER: DataItem = roger();
        static ref DAVID: DataItem = david();
        static ref DATE: Date = Date::from_calendar_date(2024, Month::January, 1).unwrap();
    }

    #[test]
    fn query_enum_eq_filter() {
        // given
        let runner = STORAGES.start_runner(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            CRISTIANO_RONALDO.clone(),
        ]);

        let filter = CompositeFilter::eq("sport", FieldValue::str("Football"));

        // when
        let mut matches = runner
            .engine
            .query(
                QueryExecution::new()
                    .for_entity(runner.name.clone())
                    .with_filter(filter),
            )
            .unwrap();

        // then
        matches.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(
            matches,
            vec![LIONEL_MESSI.clone(), CRISTIANO_RONALDO.clone()]
        );
    }

    #[test]
    fn query_bool_eq_filter() {
        // given
        let runner = STORAGES.start_runner(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            CRISTIANO_RONALDO.clone(),
        ]);

        let filter = CompositeFilter::eq("active", FieldValue::bool(false));

        // when
        let mut matches = runner
            .engine
            .query(
                QueryExecution::new()
                    .for_entity(runner.name.clone())
                    .with_filter(filter),
            )
            .unwrap();

        // then
        matches.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(matches, vec![MICHAEL_JORDAN.clone()]);
    }

    #[test]
    fn query_date_ge_filter() {
        // given
        let runner = STORAGES.start_runner(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            CRISTIANO_RONALDO.clone(),
            ROGER.clone(),
        ]);

        let filter = CompositeFilter::ge("birth_date", FieldValue::str("1990-01-01"));

        // when
        let mut matches = runner
            .engine
            .query(
                QueryExecution::new()
                    .for_entity(runner.name.clone())
                    .with_filter(filter),
            )
            .unwrap();

        // then
        matches.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(matches, vec![ROGER.clone()]);
    }

    #[test]
    fn query_date_between_filter() {
        // given
        let runner = STORAGES.start_runner(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            CRISTIANO_RONALDO.clone(),
            ROGER.clone(),
        ]);

        let filter = CompositeFilter::between(
            "birth_date",
            FieldValue::str("1970-01-01"),
            FieldValue::str("1990-01-01"),
        );

        // when
        let mut matches = runner
            .engine
            .query(
                QueryExecution::new()
                    .for_entity(runner.name.clone())
                    .with_filter(filter),
            )
            .unwrap();

        // then
        matches.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(
            matches,
            vec![LIONEL_MESSI.clone(), CRISTIANO_RONALDO.clone()]
        );
    }

    #[test]
    fn query_numeric_between_filter() {
        // given
        let runner = STORAGES.start_runner(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            ROGER.clone(),
        ]);

        let filter = CompositeFilter::between("score", FieldValue::dec(6.0), FieldValue::dec(10.0));

        // when
        let mut matches = runner
            .engine
            .query(
                QueryExecution::new()
                    .for_entity(runner.name.clone())
                    .with_filter(filter),
            )
            .unwrap();

        // then
        matches.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(matches, vec![MICHAEL_JORDAN.clone(), LIONEL_MESSI.clone()]);
    }

    #[test]
    fn query_numeric_ge_filter() {
        // given
        let runner = STORAGES.start_runner(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            ROGER.clone(),
        ]);

        let filter = CompositeFilter::ge("score", FieldValue::dec(6.0));

        // when
        let mut matches = runner
            .engine
            .query(
                QueryExecution::new()
                    .for_entity(runner.name.clone())
                    .with_filter(filter),
            )
            .unwrap();

        // then
        matches.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(matches, vec![MICHAEL_JORDAN.clone(), LIONEL_MESSI.clone()]);
    }

    #[test]
    fn query_numeric_le_filter() {
        // given
        let runner = STORAGES.start_runner(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            ROGER.clone(),
        ]);

        let filter = CompositeFilter::le("score", FieldValue::dec(6.0));

        // when
        let mut matches = runner
            .engine
            .query(
                QueryExecution::new()
                    .for_entity(runner.name.clone())
                    .with_filter(filter),
            )
            .unwrap();

        // then
        matches.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(matches, vec![ROGER.clone()]);
    }

    #[test]
    fn query_and_filter() {
        // given
        let runner = STORAGES.start_runner(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            CRISTIANO_RONALDO.clone(),
            ROGER.clone(),
        ]);

        let filter = CompositeFilter::and(vec![
            CompositeFilter::ge("score", FieldValue::dec(2.0)),
            CompositeFilter::eq("active", FieldValue::Bool(false)),
        ]);

        // when
        let mut matches = runner
            .engine
            .query(
                QueryExecution::new()
                    .for_entity(runner.name.clone())
                    .with_filter(filter),
            )
            .unwrap();

        // then
        matches.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(matches, vec![MICHAEL_JORDAN.clone(), ROGER.clone()]);
    }

    #[test]
    fn query_or_filter() {
        // given
        let runner = STORAGES.start_runner(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            CRISTIANO_RONALDO.clone(),
            ROGER.clone(),
        ]);

        let filter = CompositeFilter::or(vec![
            CompositeFilter::ge("score", FieldValue::dec(9.0)),
            CompositeFilter::le("birth_date", FieldValue::str("1990-01-01")),
        ]);

        // when
        let mut matches = runner
            .engine
            .query(
                QueryExecution::new()
                    .for_entity(runner.name.clone())
                    .with_filter(filter),
            )
            .unwrap();

        // then
        matches.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(
            matches,
            vec![
                MICHAEL_JORDAN.clone(),
                LIONEL_MESSI.clone(),
                CRISTIANO_RONALDO.clone()
            ]
        );
    }

    #[test]
    fn query_not_filter() {
        // given
        let runner = STORAGES.start_runner(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            CRISTIANO_RONALDO.clone(),
            ROGER.clone(),
        ]);

        let filter = CompositeFilter::negate(CompositeFilter::eq(
            "sport",
            FieldValue::String(Sport::Basketball.as_string()),
        ));

        // when
        let mut matches = runner
            .engine
            .query(
                QueryExecution::new()
                    .for_entity(runner.name.clone())
                    .with_filter(filter),
            )
            .unwrap();

        // then
        matches.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(
            matches,
            vec![
                LIONEL_MESSI.clone(),
                CRISTIANO_RONALDO.clone(),
                ROGER.clone(),
            ]
        );
    }

    #[test]
    fn query_numeric_delta() {
        // given
        let runner = STORAGES.start_runner(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            CRISTIANO_RONALDO.clone(),
        ]);

        let delta_scope =
            DeltaScope::date(Date::from_calendar_date(2023, Month::January, 1).unwrap());
        let deltas = vec![
            DecreaseScoreDelta::create(MICHAEL_JORDAN.id, 10.0),
            DecreaseScoreDelta::create(LIONEL_MESSI.id, 9.0),
        ];

        runner
            .engine
            .store_deltas(&runner.name, &delta_scope, deltas)
            .unwrap();

        // when
        let execution = QueryExecution::new()
            .for_entity(runner.name.clone())
            .with_filter(CompositeFilter::eq("sport", FieldValue::str("Football")))
            .with_scope(DeltaScope::date(
                Date::from_calendar_date(2024, Month::January, 1).unwrap(),
            ));

        let mut matches = runner.engine.query(execution).unwrap();

        // then
        matches.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(
            matches,
            vec![
                Player {
                    id: LIONEL_MESSI.id,
                    name: "Lionel Messi".to_string(),
                    score: Some(8.0),
                    sport: Sport::Football,
                    birth_date: "1987-06-24".to_string(),
                    active: true,
                }
                .as_item(),
                CRISTIANO_RONALDO.clone(),
            ]
        );
    }

    #[test]
    fn query_enum_delta() {
        // given
        let runner = STORAGES.start_runner(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            CRISTIANO_RONALDO.clone(),
        ]);

        let delta_scope =
            DeltaScope::date(Date::from_calendar_date(2023, Month::January, 1).unwrap());
        let deltas = vec![SwitchSportsDelta::create(
            MICHAEL_JORDAN.id,
            Sport::Football,
        )];

        runner
            .engine
            .store_deltas(&runner.name, &delta_scope, deltas)
            .unwrap();

        // when
        let execution = QueryExecution::new()
            .for_entity(runner.name.clone())
            .with_filter(CompositeFilter::eq("sport", FieldValue::str("Football")))
            .with_scope(DeltaScope::date(
                Date::from_calendar_date(2024, Month::January, 1).unwrap(),
            ));

        let mut matches = runner.engine.query(execution).unwrap();

        // then
        matches.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(
            matches,
            vec![
                Player {
                    id: 0,
                    name: "Michael Jordan".to_string(),
                    score: Some(10.0),
                    sport: Sport::Football,
                    birth_date: "1963-02-17".to_string(),
                    active: false,
                }
                .as_item(),
                LIONEL_MESSI.clone(),
                CRISTIANO_RONALDO.clone(),
            ]
        );
    }

    #[test]
    fn query_with_delta_context() {
        // given
        let runner = STORAGES.start_runner(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            CRISTIANO_RONALDO.clone(),
        ]);

        let context = 0;
        let delta_scope = DeltaScope::context(
            context,
            Date::from_calendar_date(2023, Month::January, 1).unwrap(),
        );

        let deltas = vec![SwitchSportsDelta::create(
            MICHAEL_JORDAN.id,
            Sport::Football,
        )];

        runner
            .engine
            .store_deltas(&runner.name, &delta_scope, deltas)
            .unwrap();

        // when
        let filter = CompositeFilter::eq("sport", FieldValue::str("Football"));

        let execution_without_context = QueryExecution::new()
            .for_entity(runner.name.clone())
            .with_filter(filter.clone())
            .with_scope(DeltaScope::date(
                Date::from_calendar_date(2024, Month::January, 1).unwrap(),
            ));

        let mut matches_without_context = runner.engine.query(execution_without_context).unwrap();

        // then
        matches_without_context.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(
            matches_without_context,
            vec![LIONEL_MESSI.clone(), CRISTIANO_RONALDO.clone(),]
        );

        // when
        let execution_with_context = QueryExecution::new()
            .for_entity(runner.name.clone())
            .with_filter(filter.clone())
            .with_scope(DeltaScope::context(
                context,
                Date::from_calendar_date(2024, Month::January, 1).unwrap(),
            ));

        let mut matches_with_context = runner.engine.query(execution_with_context).unwrap();

        // then
        matches_with_context.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(
            matches_with_context,
            vec![
                Player {
                    id: 0,
                    name: "Michael Jordan".to_string(),
                    score: Some(10.0),
                    sport: Sport::Football,
                    birth_date: "1963-02-17".to_string(),
                    active: false,
                }
                .as_item(),
                LIONEL_MESSI.clone(),
                CRISTIANO_RONALDO.clone(),
            ]
        );
    }

    #[test]
    fn query_pagination() {
        // given
        let runner = STORAGES.start_runner(create_random_players(20));

        let filter = CompositeFilter::eq("sport", FieldValue::str("Football"));
        let sort = Sort::new("score");
        let pagination = Pagination::new(2, 5);

        // when
        let matches = runner
            .engine
            .query(
                QueryExecution::new()
                    .for_entity(runner.name.clone())
                    .with_filter(filter)
                    .with_sort(sort)
                    .with_pagination(pagination),
            )
            .unwrap();

        // then
        assert_eq!(
            matches,
            vec![
                create_player_from_index(5),
                create_player_from_index(7),
                create_player_from_index(9),
                create_player_from_index(11),
                create_player_from_index(13),
            ]
        );
    }

    #[test]
    fn query_sort_numeric_asc() {
        // given
        let runner = STORAGES.start_runner(vec![
            MICHAEL_JORDAN.clone(),
            CRISTIANO_RONALDO.clone(),
            ROGER.clone(),
            DAVID.clone(),
        ]);

        let sort = Sort::new("score").with_direction(SortDirection::ASC);

        // when
        let matches = runner
            .engine
            .query(
                QueryExecution::new()
                    .for_entity(runner.name.clone())
                    .with_sort(sort),
            )
            .unwrap();

        // then
        assert_eq!(
            matches,
            vec![
                ROGER.clone(),
                CRISTIANO_RONALDO.clone(),
                MICHAEL_JORDAN.clone(),
                DAVID.clone(),
            ]
        );
    }

    #[test]
    fn query_sort_numeric_desc() {
        // given
        let runner = STORAGES.start_runner(vec![
            MICHAEL_JORDAN.clone(),
            CRISTIANO_RONALDO.clone(),
            ROGER.clone(),
            DAVID.clone(),
        ]);

        let sort = Sort::new("score").with_direction(SortDirection::DESC);

        // when
        let matches = runner
            .engine
            .query(
                QueryExecution::new()
                    .for_entity(runner.name.clone())
                    .with_sort(sort),
            )
            .unwrap();

        // then
        assert_eq!(
            matches,
            vec![
                MICHAEL_JORDAN.clone(),
                CRISTIANO_RONALDO.clone(),
                ROGER.clone(),
                DAVID.clone(),
            ]
        );
    }

    #[test]
    fn compute_all_filter_options() {
        // given
        let runner = STORAGES.start_runner(vec![
            MICHAEL_JORDAN.clone(),
            CRISTIANO_RONALDO.clone(),
            LIONEL_MESSI.clone(),
            ROGER.clone(),
            DAVID.clone(),
        ]);

        // when
        let mut filter_options = runner
            .engine
            .options(OptionsQueryExecution::new().for_entity(runner.name.clone()))
            .unwrap();

        // then
        filter_options.sort_by(|a, b| a.field.cmp(&b.field));

        assert_eq!(
            filter_options,
            vec![
                FilterOption::new(
                    "active".to_string(),
                    BTreeMap::from_iter([("true".to_string(), 2), ("false".to_string(), 3)])
                ),
                FilterOption::new("birth_date".to_string(), BTreeMap::from_iter([])),
                FilterOption::new(
                    "name".to_string(),
                    BTreeMap::from_iter([
                        ("Cristiano Ronaldo".to_string(), 1),
                        ("Michael Jordan".to_string(), 1),
                        ("Lionel Messi".to_string(), 1),
                        ("Roger".to_string(), 1),
                        ("David".to_string(), 1)
                    ]),
                ),
                FilterOption::new(
                    "score".to_string(),
                    BTreeMap::from_iter([
                        ("5".to_string(), 1),
                        ("9".to_string(), 2),
                        ("10".to_string(), 1)
                    ]),
                ),
                FilterOption::new(
                    "sport".to_string(),
                    BTreeMap::from_iter([
                        ("Basketball".to_string(), 2),
                        ("Football".to_string(), 3)
                    ]),
                )
            ]
        );
    }

    #[test]
    fn compute_all_filter_options_with_filter() {
        // given
        let runner = STORAGES.start_runner(vec![
            MICHAEL_JORDAN.clone(),
            CRISTIANO_RONALDO.clone(),
            LIONEL_MESSI.clone(),
            ROGER.clone(),
            DAVID.clone(),
        ]);
        let filter = CompositeFilter::ge("score", FieldValue::dec(8.0));

        // when
        let mut filter_options = runner
            .engine
            .options(
                OptionsQueryExecution::new()
                    .for_entity(runner.name.to_string())
                    .with_filter(filter),
            )
            .unwrap();

        // then
        filter_options.sort_by(|a, b| a.field.cmp(&b.field));

        assert_eq!(
            filter_options,
            vec![
                FilterOption::new(
                    "active".to_string(),
                    BTreeMap::from_iter([("true".to_string(), 2), ("false".to_string(), 1)])
                ),
                FilterOption::new("birth_date".to_string(), BTreeMap::from_iter([])),
                FilterOption::new(
                    "name".to_string(),
                    BTreeMap::from_iter([
                        ("Cristiano Ronaldo".to_string(), 1),
                        ("David".to_string(), 0),
                        ("Lionel Messi".to_string(), 1),
                        ("Michael Jordan".to_string(), 1),
                        ("Roger".to_string(), 0),
                    ]),
                ),
                FilterOption::new(
                    "score".to_string(),
                    BTreeMap::from_iter([
                        ("5".to_string(), 0),
                        ("9".to_string(), 2),
                        ("10".to_string(), 1)
                    ]),
                ),
                FilterOption::new(
                    "sport".to_string(),
                    BTreeMap::from_iter([
                        ("Basketball".to_string(), 1),
                        ("Football".to_string(), 2)
                    ]),
                )
            ]
        );
    }

    #[test]
    fn add_item() {
        // given
        let runner = STORAGES.start_runner(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            CRISTIANO_RONALDO.clone(),
        ]);

        // when
        runner.engine.add(&runner.name, &ROGER).unwrap();

        // then
        let query = QueryExecution::new()
            .for_entity(runner.name.clone())
            .with_filter(CompositeFilter::eq(
                "name",
                FieldValue::String("Roger".to_string()),
            ));
        let matches = runner.engine.query(query).unwrap();

        assert_eq!(matches, vec![ROGER.clone()]);
    }

    #[test]
    fn remove_item() {
        // given
        let runner = STORAGES.start_runner(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            CRISTIANO_RONALDO.clone(),
        ]);

        // when
        runner
            .engine
            .remove(&runner.name, &CRISTIANO_RONALDO.id)
            .unwrap();

        // then
        let query = QueryExecution::new()
            .for_entity(runner.name.clone())
            .with_filter(CompositeFilter::eq(
                "name",
                FieldValue::String("Cristiano Ronaldo".to_string()),
            ));
        let matches = runner.engine.query(query).unwrap();

        assert!(matches.is_empty());
    }
}
