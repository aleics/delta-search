#![feature(iter_array_chunks)]
#![feature(iter_intersperse)]

use crate::data::{DataItem, DataItemId};
use crate::query::{FilterOption, OptionsQueryExecution, QueryExecution};
use crate::storage::{CreateFieldIndex, EntityStorage};
use std::slice;

pub mod data;
#[cfg(feature = "test-fixtures")]
pub mod fixtures;
pub mod index;
pub mod query;
pub mod storage;

pub struct Engine {
    storage: EntityStorage,
}

impl Engine {
    pub fn new(storage: EntityStorage) -> Self {
        Engine { storage }
    }

    pub fn query(&self, execution: QueryExecution) -> Vec<DataItem> {
        execution.run(&self.storage)
    }

    pub fn options(&self, execution: OptionsQueryExecution) -> Vec<FilterOption> {
        execution.run(&self.storage)
    }

    pub fn add(&mut self, item: &DataItem) {
        self.storage.add(slice::from_ref(item));
    }

    pub fn remove(&mut self, id: &DataItemId) {
        self.storage.remove(slice::from_ref(id));
    }

    pub fn clear(&mut self) {
        self.storage.clear()
    }

    pub fn create_index(&mut self, command: CreateFieldIndex) {
        self.storage.create_indices(vec![command]);
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
            .query(QueryExecution::new().with_filter(filter));

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
            .query(QueryExecution::new().with_filter(filter));

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
            .query(QueryExecution::new().with_filter(filter));

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
            .query(QueryExecution::new().with_filter(filter));

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
            .query(QueryExecution::new().with_filter(filter));

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
            .query(QueryExecution::new().with_filter(filter));

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
            .query(QueryExecution::new().with_filter(filter));

        // then
        matches.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(matches, vec![ROGER.clone()]);
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
            .query(QueryExecution::new().with_filter(filter));

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
        let matches = runner.engine.query(QueryExecution::new().with_sort(sort));

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
        let matches = runner.engine.query(QueryExecution::new().with_sort(sort));

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
        let mut filter_options = runner.engine.options(OptionsQueryExecution::new());

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
        let mut filter_options = runner
            .engine
            .options(OptionsQueryExecution::new().with_filter(filter));

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
        runner.engine.add(&ROGER);

        // then
        let query = QueryExecution::new().with_filter(CompositeFilter::eq(
            "name",
            FieldValue::String("Roger".to_string()),
        ));
        let matches = runner.engine.query(query);

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
        runner.engine.remove(&CRISTIANO_RONALDO.id);

        // then
        let query = QueryExecution::new().with_filter(CompositeFilter::eq(
            "name",
            FieldValue::String("Cristiano Ronaldo".to_string()),
        ));
        let matches = runner.engine.query(query);

        assert!(matches.is_empty());
    }
}
