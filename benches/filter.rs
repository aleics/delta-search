#![feature(test)]
extern crate test;

use delta_db::fixtures::{create_players_storage, create_random_players, Sport};
use delta_db::query::{CompositeFilter, Pagination, QueryExecution, Sort, SortDirection};
use delta_db::{Engine, FieldValue};
use lazy_static::lazy_static;
use test::Bencher;

const COUNT: usize = 100000;
const PAGE_SIZE: usize = 5000;

lazy_static! {
    static ref PAGINATION: Pagination = Pagination::new(0, PAGE_SIZE);
}

#[bench]
fn bench_filter_numeric_eq(b: &mut Bencher) {
    let engine = Engine::new(create_players_storage(create_random_players(COUNT)));

    b.iter(move || {
        let filter = CompositeFilter::eq("score", FieldValue::numeric(0.0));
        let query = QueryExecution::new()
            .with_filter(filter)
            .with_pagination(*PAGINATION);

        engine.query(query);
    });
}

#[bench]
fn bench_filter_numeric_between(b: &mut Bencher) {
    let engine = Engine::new(create_players_storage(create_random_players(COUNT)));

    b.iter(move || {
        let filter =
            CompositeFilter::between("score", FieldValue::numeric(0.0), FieldValue::numeric(5.0));
        let query = QueryExecution::new()
            .with_filter(filter)
            .with_pagination(*PAGINATION);

        engine.query(query);
    });
}

#[bench]
fn bench_filter_or(b: &mut Bencher) {
    let engine = Engine::new(create_players_storage(create_random_players(COUNT)));

    b.iter(move || {
        let filter = CompositeFilter::and(vec![
            CompositeFilter::eq("sport", FieldValue::String(Sport::Basketball.as_string())),
            CompositeFilter::between("score", FieldValue::numeric(0.0), FieldValue::numeric(5.0)),
        ]);
        let query = QueryExecution::new()
            .with_filter(filter)
            .with_pagination(*PAGINATION);

        engine.query(query);
    });
}

#[bench]
fn bench_sort(b: &mut Bencher) {
    let engine = Engine::new(create_players_storage(create_random_players(COUNT)));

    b.iter(move || {
        let sort = Sort::new("score").with_direction(SortDirection::DESC);
        let query = QueryExecution::new()
            .with_sort(sort)
            .with_pagination(*PAGINATION);

        engine.query(query);
    });
}
