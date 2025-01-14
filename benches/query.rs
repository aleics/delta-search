#![feature(test)]
extern crate test;

use test::Bencher;

use lazy_static::lazy_static;

use delta_search::data::{DataItem, FieldValue};
use delta_search::fixtures::{
    create_players_storage, create_random_players, decrease_score_deltas, switch_sports_deltas,
    Sport,
};
use delta_search::query::{
    CompositeFilter, OptionsQueryExecution, Pagination, QueryExecution, QueryScope, Sort,
    SortDirection,
};
use delta_search::Engine;
use time::{Date, Month};

const COUNT: usize = 10000;
const PAGE_SIZE: usize = 500;

lazy_static! {
    static ref PAGINATION: Pagination = Pagination::new(0, PAGE_SIZE);
    static ref NAME: String = "players_bench".to_string();
    static ref DATE: Date = Date::from_calendar_date(2023, Month::January, 1).unwrap();
    static ref PLAYERS: Vec<DataItem> = create_random_players(COUNT as u64);
    static ref ENGINE: Engine =
        Engine::with_entities(vec![create_players_storage(&NAME, PLAYERS.to_vec())]);
}

#[bench]
fn bench_filter_numeric_eq(b: &mut Bencher) {
    b.iter(move || {
        tokio_test::block_on(async {
            let filter = CompositeFilter::eq("score", FieldValue::dec(10.0));
            let query = QueryExecution::new()
                .with_filter(filter)
                .with_pagination(*PAGINATION);

            ENGINE.query(&NAME, query).await.unwrap();
        });
    });
}

#[bench]
fn bench_filter_numeric_between(b: &mut Bencher) {
    b.iter(move || {
        tokio_test::block_on(async {
            let filter =
                CompositeFilter::between("score", FieldValue::dec(0.0), FieldValue::dec(100.0));
            let query = QueryExecution::new()
                .with_filter(filter)
                .with_pagination(*PAGINATION);

            ENGINE.query(&NAME, query).await.unwrap();
        });
    });
}

#[bench]
fn bench_filter_or(b: &mut Bencher) {
    b.iter(move || {
        tokio_test::block_on(async {
            let filter = CompositeFilter::or(vec![
                CompositeFilter::eq("sport", FieldValue::String(Sport::Basketball.as_string())),
                CompositeFilter::between("score", FieldValue::dec(0.0), FieldValue::dec(100.0)),
            ]);
            let query = QueryExecution::new()
                .with_filter(filter)
                .with_pagination(*PAGINATION);

            ENGINE.query(&NAME, query).await.unwrap();
        });
    });
}

#[bench]
fn bench_sort(b: &mut Bencher) {
    b.iter(move || {
        tokio_test::block_on(async {
            let sort = Sort::new("score").with_direction(SortDirection::DESC);
            let query = QueryExecution::new()
                .with_sort(sort)
                .with_pagination(*PAGINATION);

            ENGINE.query(&NAME, query).await.unwrap();
        });
    });
}

#[bench]
fn bench_filter_options(b: &mut Bencher) {
    b.iter(move || {
        tokio_test::block_on(async {
            ENGINE
                .options(&NAME, OptionsQueryExecution::new())
                .await
                .unwrap();
        });
    });
}

#[bench]
fn bench_apply_deltas(b: &mut Bencher) {
    let mut deltas = Vec::new();

    deltas.extend(decrease_score_deltas(&PLAYERS, COUNT));
    deltas.extend(switch_sports_deltas(&PLAYERS, COUNT));

    tokio_test::block_on(async {
        ENGINE.store_deltas(&NAME, *DATE, &deltas).await.unwrap();
    });

    b.iter(move || {
        tokio_test::block_on(async {
            let query = QueryExecution::new()
                .with_scope(QueryScope::date(DATE.next_day().unwrap()))
                .with_pagination(*PAGINATION);

            ENGINE.query(&NAME, query).await.unwrap();
        });
    });
}
