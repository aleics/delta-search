use delta_search::data::FieldValue;
use delta_search::fixtures::{
    create_players_storage, cristiano_ronaldo, david, lionel_messi, michael_jordan, roger, Sport,
    SwitchSportsDelta,
};
use delta_search::query::{
    CompositeFilter, OptionsQueryExecution, QueryExecution, Sort, SortDirection,
};
use delta_search::Engine;
use time::{Date, Month};

#[tokio::main]
async fn main() {
    println!("Welcome to the simple Player search!");

    let michael_jordan = michael_jordan();
    let lionel_messi = lionel_messi();
    let cristiano_ronaldo = cristiano_ronaldo();
    let roger = roger();
    let david = david();

    let michael_jordan_id = michael_jordan.id;
    let lionel_messi_id = lionel_messi.id;
    let david_id = david.id;

    let name = "players_example_simple";

    let entity = create_players_storage(
        name,
        vec![
            michael_jordan,
            lionel_messi,
            cristiano_ronaldo,
            roger,
            david,
        ],
    );

    let engine = Engine::with_entities(vec![entity]);

    let filter_options = engine.options(name, OptionsQueryExecution::new()).await;

    println!("Filter possibilities:\n{:?}\n", filter_options);

    let query = QueryExecution::new()
        .with_filter(CompositeFilter::eq(
            "sport",
            FieldValue::String(Sport::Basketball.as_string()),
        ))
        .with_sort(Sort::new("score").with_direction(SortDirection::DESC));
    let players = engine.query(name, query).await;

    println!("Basketball players sorted by score: {:?}", players);

    let players = engine
        .query(
            name,
            QueryExecution::new()
                .with_filter(CompositeFilter::between(
                    "birth_date",
                    FieldValue::str("1980-01-01"),
                    FieldValue::str("1989-12-31"),
                ))
                .with_sort(Sort::new("name").with_direction(SortDirection::ASC)),
        )
        .await;

    println!("Players born in the 80s: {:?}", players);

    let switch_sports = vec![
        SwitchSportsDelta::create(michael_jordan_id, Sport::Basketball, Sport::Football),
        SwitchSportsDelta::create(lionel_messi_id, Sport::Football, Sport::Basketball),
    ];

    let date = Date::from_calendar_date(2023, Month::January, 1).unwrap();

    engine
        .store_deltas(name, date, &switch_sports)
        .await
        .unwrap();

    let query = QueryExecution::new()
        .with_filter(CompositeFilter::eq(
            "sport",
            FieldValue::String(Sport::Basketball.as_string()),
        ))
        .with_sort(Sort::new("score").with_direction(SortDirection::DESC))
        .with_date(Date::from_calendar_date(2024, Month::January, 1).unwrap());

    let players = engine.query(name, query).await;

    println!(
        "Basketball players sorted by score after switching sports in 2023: {:?}",
        players
    );

    engine.remove(name, &david_id).await.unwrap();

    let players = engine
        .query(
            name,
            QueryExecution::new().with_filter(CompositeFilter::eq(
                "sport",
                FieldValue::String(Sport::Basketball.as_string()),
            )),
        )
        .await;

    println!("Players playing basketball after deletion: {:?}", players);
}
