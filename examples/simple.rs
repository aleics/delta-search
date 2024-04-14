use delta_search::data::FieldValue;
use delta_search::fixtures::{
    create_players_storage, cristiano_ronaldo, david, lionel_messi, michael_jordan, roger, Sport,
    SwitchSportsDelta,
};
use delta_search::query::{
    CompositeFilter, OptionsQueryExecution, QueryExecution, Sort, SortDirection,
};
use delta_search::Engine;

fn main() {
    println!("Welcome to the simple Player search!");

    let michael_jordan = michael_jordan();
    let lionel_messi = lionel_messi();
    let cristiano_ronaldo = cristiano_ronaldo();
    let roger = roger();
    let david = david();

    let michael_jordan_id = michael_jordan.id;
    let lionel_messi_id = lionel_messi.id;

    let storage = create_players_storage(
        "players_example_simple",
        vec![
            michael_jordan,
            lionel_messi,
            cristiano_ronaldo,
            roger,
            david,
        ],
    );

    let mut engine = Engine::new(storage);

    let filter_options = engine.options(OptionsQueryExecution::new());

    println!("Filter possibilities:\n{:?}\n", filter_options);

    let query = QueryExecution::new()
        .with_filter(CompositeFilter::eq(
            "sport",
            FieldValue::String(Sport::Basketball.as_string()),
        ))
        .with_sort(Sort::new("score").with_direction(SortDirection::DESC));
    let players = engine.query(query);

    println!("Basketball players sorted by score: {:?}", players);

    let players = engine.query(
        QueryExecution::new()
            .with_filter(CompositeFilter::between(
                "birth_date",
                FieldValue::str("1980-01-01"),
                FieldValue::str("1989-12-31"),
            ))
            .with_sort(Sort::new("name").with_direction(SortDirection::ASC)),
    );

    println!("Players born in the 80s: {:?}", players);

    let switch_sports = vec![
        SwitchSportsDelta::create(michael_jordan_id, Sport::Basketball, Sport::Football),
        SwitchSportsDelta::create(lionel_messi_id, Sport::Football, Sport::Basketball),
    ];

    let query = QueryExecution::new()
        .with_filter(CompositeFilter::eq(
            "sport",
            FieldValue::String(Sport::Basketball.as_string()),
        ))
        .with_sort(Sort::new("score").with_direction(SortDirection::DESC))
        .with_deltas(switch_sports);

    let players = engine.query(query);

    println!(
        "Basketball players sorted by score after switching sports: {:?}",
        players
    );

    engine.remove(&4);

    println!("Basketball players sorted by score: {:?}", players);

    let players = engine.query(QueryExecution::new().with_filter(CompositeFilter::eq(
        "sport",
        FieldValue::String(Sport::Basketball.as_string()),
    )));

    println!("Players playing basketball after deletion: {:?}", players);
}
