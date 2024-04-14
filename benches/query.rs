#![feature(test)]
extern crate test;

use test::Bencher;

use lazy_static::lazy_static;

use delta_search::data::FieldValue;
use delta_search::fixtures::{
    create_players_storage, create_random_players, decrease_score_deltas, switch_sports_deltas,
    Player, Sport,
};
use delta_search::query::{
    CompositeFilter, OptionsQueryExecution, Pagination, QueryExecution, Sort, SortDirection,
};
use delta_search::Engine;

const COUNT: usize = 10000;
const PAGE_SIZE: usize = 500;

lazy_static! {
    static ref PAGINATION: Pagination = Pagination::new(0, PAGE_SIZE);
    static ref ENGINE: Engine = Engine::new(create_players_storage(
        "players_bench",
        create_random_players(COUNT)
    ));
}

#[bench]
fn bench_filter_numeric_eq(b: &mut Bencher) {
    b.iter(move || {
        let filter = CompositeFilter::eq("score", FieldValue::dec(10.0));
        let query = QueryExecution::new()
            .with_filter(filter)
            .with_pagination(*PAGINATION);

        ENGINE.query(query);
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

        ENGINE.query(query);
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

        ENGINE.query(query);
    });
}

#[bench]
fn bench_sort(b: &mut Bencher) {
    b.iter(move || {
        let sort = Sort::new("score").with_direction(SortDirection::DESC);
        let query = QueryExecution::new()
            .with_sort(sort)
            .with_pagination(*PAGINATION);

        ENGINE.query(query);
    });
}

#[bench]
fn bench_filter_options(b: &mut Bencher) {
    b.iter(move || ENGINE.options(OptionsQueryExecution::new()));
}

#[bench]
fn bench_apply_deltas(b: &mut Bencher) {
    let players = create_random_players(COUNT);
    let decrease_score_deltas = decrease_score_deltas(&players, COUNT);
    let switch_sports_deltas = switch_sports_deltas(&players, COUNT);

    b.iter(move || {
        let query = QueryExecution::new()
            .with_deltas(decrease_score_deltas.clone())
            .with_deltas(switch_sports_deltas.clone())
            .with_pagination(*PAGINATION);

        ENGINE.query(query);
    });
}
