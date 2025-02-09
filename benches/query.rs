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
    CompositeFilter, DeltaScope, OptionsQueryExecution, Pagination, QueryExecution, Sort,
    SortDirection,
};
use delta_search::Engine;
use time::{Date, Month};

const COUNT: usize = 100000;
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
        let filter = CompositeFilter::eq("score", FieldValue::dec(10.0));
        let query = QueryExecution::new()
            .with_filter(filter)
            .with_pagination(*PAGINATION);

        ENGINE.query(&NAME, query).unwrap();
    });
}

#[bench]
fn bench_filter_numeric_between(b: &mut Bencher) {
    b.iter(move || {
        let filter =
            CompositeFilter::between("score", FieldValue::dec(0.0), FieldValue::dec(100.0));
        let query = QueryExecution::new()
            .with_filter(filter)
            .with_pagination(*PAGINATION);

        ENGINE.query(&NAME, query).unwrap();
    });
}

#[bench]
fn bench_filter_or(b: &mut Bencher) {
    b.iter(move || {
        let filter = CompositeFilter::or(vec![
            CompositeFilter::eq("sport", FieldValue::String(Sport::Basketball.as_string())),
            CompositeFilter::between("score", FieldValue::dec(0.0), FieldValue::dec(100.0)),
        ]);
        let query = QueryExecution::new()
            .with_filter(filter)
            .with_pagination(*PAGINATION);

        ENGINE.query(&NAME, query).unwrap();
    });
}

#[bench]
fn bench_sort(b: &mut Bencher) {
    b.iter(move || {
        let sort = Sort::new("score").with_direction(SortDirection::DESC);
        let query = QueryExecution::new()
            .with_sort(sort)
            .with_pagination(*PAGINATION);

        ENGINE.query(&NAME, query).unwrap();
    });
}

#[bench]
fn bench_filter_options(b: &mut Bencher) {
    b.iter(move || {
        ENGINE.options(&NAME, OptionsQueryExecution::new()).unwrap();
    });
}

#[bench]
fn bench_apply_deltas(b: &mut Bencher) {
    let mut deltas = Vec::new();

    deltas.extend(decrease_score_deltas(&PLAYERS, COUNT));
    deltas.extend(switch_sports_deltas(&PLAYERS, COUNT));

    let scope = DeltaScope::date(*DATE);
    ENGINE.store_deltas(&NAME, &scope, deltas).unwrap();

    b.iter(move || {
        let scope = DeltaScope::date(DATE.next_day().unwrap());

        let query = QueryExecution::new()
            .with_scope(scope)
            .with_pagination(*PAGINATION);

        ENGINE.query(&NAME, query).unwrap();
    });
}

#[bench]
fn bench_apply_deltas_with_multiple_dates(b: &mut Bencher) {
    let mut deltas = Vec::new();

    deltas.extend(decrease_score_deltas(&PLAYERS, COUNT));
    deltas.extend(switch_sports_deltas(&PLAYERS, COUNT));

    let mut date: Date = *DATE;

    for delta_chunk in deltas.chunks(100) {
        ENGINE
            .store_deltas(&NAME, &DeltaScope::date(date), delta_chunk.to_vec())
            .unwrap();

        date = date.next_day().unwrap();
    }

    b.iter(move || {
        let scope = DeltaScope::date(date.next_day().unwrap());

        let query = QueryExecution::new()
            .with_scope(scope)
            .with_pagination(*PAGINATION);

        ENGINE.query(&NAME, query).unwrap();
    });
}
