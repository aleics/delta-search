#![feature(iter_array_chunks)]
#![feature(iter_intersperse)]

use std::collections::HashMap;
use std::slice;

use crate::data::{DataItem, DataItemId};
use crate::query::{FilterOption, OptionsQueryExecution, QueryExecution};
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
    pub fn init() -> Self {
        let mut entities = HashMap::new();

        for name in storage::read_stored_entity_names() {
            let storage = StorageBuilder::new(&name).build();
            entities.insert(name, storage);
        }

        Engine { entities }
    }

    pub fn with_entities(entries: Vec<EntityStorage>) -> Self {
        let mut entities = HashMap::new();
        for entry in entries {
            entities.insert(entry.id.clone(), entry);
        }
        Engine { entities }
    }

    pub fn create_entity(&mut self, name: String) {
        if self.entities.contains_key(&name) {
            panic!("Entity with name \"{}\" already exists", name);
        }

        let entity = StorageBuilder::new(&name).build();
        self.entities.insert(name, entity);
    }

    pub fn query(&self, name: &str, execution: QueryExecution) -> Vec<DataItem> {
        if let Some(entity) = self.entities.get(name) {
            execution.run(entity)
        } else {
            Vec::new()
        }
    }

    pub fn options(&self, name: &str, execution: OptionsQueryExecution) -> Vec<FilterOption> {
        if let Some(entity) = self.entities.get(name) {
            execution.run(entity)
        } else {
            Vec::new()
        }
    }

    pub fn add(&mut self, name: &str, item: &DataItem) {
        if let Some(entity) = self.entities.get_mut(name) {
            entity.add(slice::from_ref(item));
        }
    }

    pub fn remove(&mut self, name: &str, id: &DataItemId) {
        if let Some(entity) = self.entities.get_mut(name) {
            entity.remove(slice::from_ref(id));
        }
    }

    pub fn clear(&mut self, name: &str) {
        if let Some(entity) = self.entities.get_mut(name) {
            entity.clear();
        }
    }

    pub fn create_index(&mut self, name: &str, command: CreateFieldIndex) {
        if let Some(entity) = self.entities.get_mut(name) {
            entity.create_indices(vec![command]);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use lazy_static::lazy_static;

    use crate::data::{DataItem, FieldValue};
    use crate::fixtures::{
        create_player_from_index, create_random_players, cristiano_ronaldo, david, lionel_messi,
        michael_jordan, roger, DecreaseScoreDelta, Player, Sport, SwitchSportsDelta, TestRunners,
    };
    use crate::query::{
        CompositeFilter, FilterOption, OptionsQueryExecution, Pagination, QueryExecution, Sort,
        SortDirection,
    };

    lazy_static! {
        static ref STORAGES: TestRunners = TestRunners::start(24);
    }

    lazy_static! {
        static ref MICHAEL_JORDAN: DataItem = michael_jordan();
        static ref LIONEL_MESSI: DataItem = lionel_messi();
        static ref CRISTIANO_RONALDO: DataItem = cristiano_ronaldo();
        static ref ROGER: DataItem = roger();
        static ref DAVID: DataItem = david();
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
            .query(&runner.name, QueryExecution::new().with_filter(filter));

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
            .query(&runner.name, QueryExecution::new().with_filter(filter));

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
            .query(&runner.name, QueryExecution::new().with_filter(filter));

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
            .query(&runner.name, QueryExecution::new().with_filter(filter));

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
            .query(&runner.name, QueryExecution::new().with_filter(filter));

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
            .query(&runner.name, QueryExecution::new().with_filter(filter));

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
            .query(&runner.name, QueryExecution::new().with_filter(filter));

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
            .query(&runner.name, QueryExecution::new().with_filter(filter));

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
            .query(&runner.name, QueryExecution::new().with_filter(filter));

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
            .query(&runner.name, QueryExecution::new().with_filter(filter));

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

        let deltas = vec![
            DecreaseScoreDelta::create(0, 10.0),
            DecreaseScoreDelta::create(1, 9.0),
        ];
        let filter = CompositeFilter::eq("sport", FieldValue::str("Football"));

        // when
        let mut matches = runner.engine.query(
            &runner.name,
            QueryExecution::new()
                .with_filter(filter)
                .with_deltas(deltas),
        );

        // then
        matches.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(
            matches,
            vec![
                Player {
                    id: 1,
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

        let deltas = vec![SwitchSportsDelta::create(
            0,
            Sport::Basketball,
            Sport::Football,
        )];
        let filter = CompositeFilter::eq("sport", FieldValue::str("Football"));

        // when
        let mut matches = runner.engine.query(
            &runner.name,
            QueryExecution::new()
                .with_filter(filter)
                .with_deltas(deltas),
        );

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
    fn query_pagination() {
        // given
        let runner = STORAGES.start_runner(create_random_players(20));

        let filter = CompositeFilter::eq("sport", FieldValue::str("Football"));
        let sort = Sort::new("score");
        let pagination = Pagination::new(2, 5);

        // when
        let matches = runner.engine.query(
            &runner.name,
            QueryExecution::new()
                .with_filter(filter)
                .with_sort(sort)
                .with_pagination(pagination),
        );

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
            .query(&runner.name, QueryExecution::new().with_sort(sort));

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
            .query(&runner.name, QueryExecution::new().with_sort(sort));

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
            .options(&runner.name, OptionsQueryExecution::new());

        // then
        filter_options.sort_by(|a, b| a.field.cmp(&b.field));

        assert_eq!(
            filter_options,
            vec![
                FilterOption::new(
                    "active".to_string(),
                    HashMap::from_iter([("true".to_string(), 2), ("false".to_string(), 3)])
                ),
                FilterOption::new("birth_date".to_string(), HashMap::from_iter([])),
                FilterOption::new(
                    "name".to_string(),
                    HashMap::from_iter([
                        ("Cristiano Ronaldo".to_string(), 1),
                        ("Michael Jordan".to_string(), 1),
                        ("Lionel Messi".to_string(), 1),
                        ("Roger".to_string(), 1),
                        ("David".to_string(), 1)
                    ]),
                ),
                FilterOption::new(
                    "score".to_string(),
                    HashMap::from_iter([
                        ("5".to_string(), 1),
                        ("9".to_string(), 2),
                        ("10".to_string(), 1)
                    ]),
                ),
                FilterOption::new(
                    "sport".to_string(),
                    HashMap::from_iter([
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
        let mut filter_options = runner.engine.options(
            &runner.name,
            OptionsQueryExecution::new().with_filter(filter),
        );

        // then
        filter_options.sort_by(|a, b| a.field.cmp(&b.field));

        assert_eq!(
            filter_options,
            vec![
                FilterOption::new(
                    "active".to_string(),
                    HashMap::from_iter([("true".to_string(), 2), ("false".to_string(), 1)])
                ),
                FilterOption::new("birth_date".to_string(), HashMap::from_iter([])),
                FilterOption::new(
                    "name".to_string(),
                    HashMap::from_iter([
                        ("Cristiano Ronaldo".to_string(), 1),
                        ("Michael Jordan".to_string(), 1),
                        ("Lionel Messi".to_string(), 1)
                    ]),
                ),
                FilterOption::new(
                    "score".to_string(),
                    HashMap::from_iter([("9".to_string(), 2), ("10".to_string(), 1)]),
                ),
                FilterOption::new(
                    "sport".to_string(),
                    HashMap::from_iter([
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
        let mut runner = STORAGES.start_runner(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            CRISTIANO_RONALDO.clone(),
        ]);

        // when
        runner.engine.add(&runner.name, &ROGER);

        // then
        let query = QueryExecution::new().with_filter(CompositeFilter::eq(
            "name",
            FieldValue::String("Roger".to_string()),
        ));
        let matches = runner.engine.query(&runner.name, query);

        assert_eq!(matches, vec![ROGER.clone()]);
    }

    #[test]
    fn remove_item() {
        // given
        let mut runner = STORAGES.start_runner(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            CRISTIANO_RONALDO.clone(),
        ]);

        // when
        runner.engine.remove(&runner.name, &CRISTIANO_RONALDO.id);

        // then
        let query = QueryExecution::new().with_filter(CompositeFilter::eq(
            "name",
            FieldValue::String("Cristiano Ronaldo".to_string()),
        ));
        let matches = runner.engine.query(&runner.name, query);

        assert!(matches.is_empty());
    }
}
