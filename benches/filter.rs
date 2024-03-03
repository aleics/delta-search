#![feature(test)]
extern crate test;

use delta_db::fixtures::{Player, Sport};
use delta_db::query::{CompositeFilter, Pagination, QueryExecution};
use delta_db::{Engine, EntityStorage, FieldValue};
use rand::Rng;
use test::Bencher;

fn create_random_players(count: usize) -> Vec<Player> {
    (0..count)
        .into_iter()
        .map(create_player_from_index)
        .collect()
}

fn create_player_from_index(index: usize) -> Player {
    let score = rand::thread_rng().gen_range(0.0..10.0);
    Player {
        id: index,
        name: format!("Player {}", index),
        score,
        sport: if index % 2 == 0 {
            Sport::Basketball
        } else {
            Sport::Football
        },
        birth_date: "1999-12-31".to_string(),
    }
}

fn storage(data: Vec<Player>) -> EntityStorage<Player> {
    let mut storage = EntityStorage::new();

    storage.attach(data);
    storage.index();

    storage
}

const COUNT: usize = 100000;
const PAGE_SIZE: usize = 5000;

#[bench]
fn bench_filter_numeric_eq(b: &mut Bencher) {
    let engine = Engine::new(storage(create_random_players(COUNT)));

    b.iter(move || {
        let filter = CompositeFilter::eq("score", FieldValue::numeric(0.0));
        engine.query(QueryExecution::new(filter).with_pagination(Pagination::new(0, PAGE_SIZE)));
    });
}

#[bench]
fn bench_filter_numeric_between(b: &mut Bencher) {
    let engine = Engine::new(storage(create_random_players(COUNT)));

    b.iter(move || {
        let filter =
            CompositeFilter::between("score", FieldValue::numeric(0.0), FieldValue::numeric(5.0));
        engine.query(QueryExecution::new(filter).with_pagination(Pagination::new(0, PAGE_SIZE)));
    });
}

#[bench]
fn bench_filter_or(b: &mut Bencher) {
    let engine = Engine::new(storage(create_random_players(COUNT)));

    b.iter(move || {
        let filter = CompositeFilter::and(vec![
            CompositeFilter::eq("sport", FieldValue::String(Sport::Basketball.as_string())),
            CompositeFilter::between("score", FieldValue::numeric(0.0), FieldValue::numeric(5.0)),
        ]);
        engine.query(QueryExecution::new(filter).with_pagination(Pagination::new(0, PAGE_SIZE)));
    });
}
