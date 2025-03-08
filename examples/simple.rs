use anyhow::Error;
use delta_search::fixtures::{
    create_players_storage, cristiano_ronaldo, david, lionel_messi, michael_jordan, roger,
    DecreaseScoreDelta, Sport, SwitchSportsDelta,
};
use delta_search::query::{DeltaScope, OptionsQueryExecution, QueryExecution, Sort, SortDirection};
use delta_search::Engine;
use time::{Date, Month};

fn main() -> Result<(), Error> {
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

    let filter_options = engine.options(OptionsQueryExecution::new().for_entity(name.to_string()));

    println!("Filter possibilities:\n{:?}\n", filter_options);

    let query = QueryExecution::parse_query(&format!(
        "FROM {name} WHERE sport = \"Basketball\" ORDER BY score DESC"
    ))?;
    let players = engine.query(query);

    println!("Basketball players sorted by score:\n{:?}\n", players);

    let players = engine.query(
        QueryExecution::parse_query(&format!(
            "FROM {name} WHERE birth_date >= \"1980-01-01\" && birth_date <= \"1989-12-31\" ORDER BY name ASC"
        ))?
    );

    println!("Players born in the 80s:\n{:?}\n", players);

    let switch_sports = vec![
        SwitchSportsDelta::create(michael_jordan_id, Sport::Football),
        SwitchSportsDelta::create(lionel_messi_id, Sport::Basketball),
    ];

    let delta_scope = DeltaScope::date(Date::from_calendar_date(2023, Month::January, 1)?);

    engine
        .store_deltas(name, &delta_scope, switch_sports)
        .unwrap();

    let query = QueryExecution::parse_query(&format!(
        "FROM {name} WHERE sport = \"Basketball\" ORDER BY score DESC"
    ))?
    .with_scope(DeltaScope::date(Date::from_calendar_date(
        2024,
        Month::January,
        1,
    )?));

    let players = engine.query(query)?;

    println!(
        "Basketball players sorted by score after switching sports in 2023:\n{:?}\n",
        players
    );

    let lower_scores = vec![
        DecreaseScoreDelta::create(michael_jordan_id, 10.0),
        DecreaseScoreDelta::create(lionel_messi_id, 9.0),
    ];

    let delta_scope = DeltaScope::context(0, Date::from_calendar_date(2023, Month::January, 1)?);

    engine.store_deltas(name, &delta_scope, lower_scores)?;

    let query = QueryExecution::new()
        .for_entity(name.to_string())
        .with_sort(Sort::new("score").with_direction(SortDirection::DESC))
        .with_scope(DeltaScope::context(
            0,
            Date::from_calendar_date(2024, Month::January, 1)?,
        ));

    let players = engine.query(query)?;

    println!(
        "Players sorted by score after decreasing their score by 1:\n{:?}\n",
        players
    );

    engine.remove(name, &david_id)?;

    let players = engine.query(QueryExecution::parse_query(&format!(
        "FROM {name} WHERE sport = \"Basketball\"",
    ))?)?;

    println!(
        "Players playing basketball after deletion:\n{:?}\n",
        players
    );

    Ok(())
}
