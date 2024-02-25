pub(crate) mod index;

use ordered_float::OrderedFloat;
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};

use crate::index::Indexable;
use index::Index;
use roaring::RoaringBitmap;

#[derive(Debug, Clone, PartialEq, Eq)]
enum FieldValue {
    String(String),
    Numeric(OrderedFloat<f64>),
}

impl FieldValue {
    fn string(value: String) -> FieldValue {
        FieldValue::String(value)
    }

    fn numeric(value: f64) -> FieldValue {
        FieldValue::Numeric(OrderedFloat(value))
    }

    fn get_numeric(&self) -> Option<&OrderedFloat<f64>> {
        match self {
            FieldValue::String(_) => None,
            FieldValue::Numeric(value) => Some(value),
        }
    }

    fn get_string(&self) -> Option<&String> {
        match self {
            FieldValue::String(value) => Some(value),
            FieldValue::Numeric(_) => None,
        }
    }
}

impl Display for FieldValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FieldValue::String(value) => write!(f, "{}", value),
            FieldValue::Numeric(value) => write!(f, "{}", value.0),
        }
    }
}

type DataItemId = usize;

#[derive(Debug)]
enum CompositeFilter {
    And(Vec<CompositeFilter>),
    Or(Vec<CompositeFilter>),
    Single(Filter),
}

impl CompositeFilter {
    fn eq(name: &str, value: FieldValue) -> Self {
        CompositeFilter::Single(Filter {
            name: name.to_string(),
            operation: FilterOperation::Eq(value),
        })
    }

    fn between(name: &str, first: FieldValue, second: FieldValue) -> Self {
        CompositeFilter::Single(Filter {
            name: name.to_string(),
            operation: FilterOperation::Between(first, second),
        })
    }

    fn gt(name: &str, value: FieldValue) -> Self {
        CompositeFilter::Single(Filter {
            name: name.to_string(),
            operation: FilterOperation::GreaterThan(value),
        })
    }

    fn ge(name: &str, value: FieldValue) -> Self {
        CompositeFilter::Single(Filter {
            name: name.to_string(),
            operation: FilterOperation::GreaterOrEqual(value),
        })
    }

    fn lt(name: &str, value: FieldValue) -> Self {
        CompositeFilter::Single(Filter {
            name: name.to_string(),
            operation: FilterOperation::LessThan(value),
        })
    }

    fn le(name: &str, value: FieldValue) -> Self {
        CompositeFilter::Single(Filter {
            name: name.to_string(),
            operation: FilterOperation::LessThanOrEqual(value),
        })
    }

    fn apply(&self, indices: &HashMap<String, Index>) -> Option<FilterResult> {
        match self {
            CompositeFilter::And(filters) => filters.iter().fold(None, |acc, filter| {
                let filter_result = filter.apply(indices);
                combine_by(filter_result, acc, |inner, current| current.and(inner))
            }),
            CompositeFilter::Or(filters) => filters.iter().fold(None, |acc, filter| {
                let filter_result = filter.apply(indices);
                combine_by(filter_result, acc, |inner, current| current.or(inner))
            }),
            CompositeFilter::Single(filter) => {
                let index = indices.get(&filter.name)?;
                index.filter(&filter.operation)
            }
        }
    }
}

fn combine_by<T, F>(first: Option<T>, second: Option<T>, action: F) -> Option<T>
where
    F: Fn(T, T) -> T,
{
    match (first, second) {
        (Some(first), Some(second)) => Some(action(first, second)),
        (Some(first), None) => Some(first),
        (None, Some(second)) => Some(second),
        (None, None) => None,
    }
}

#[derive(Debug)]
struct Filter {
    name: String,
    operation: FilterOperation,
}

#[derive(Debug)]
enum FilterOperation {
    Eq(FieldValue),
    Between(FieldValue, FieldValue),
    GreaterThan(FieldValue),
    GreaterOrEqual(FieldValue),
    LessThan(FieldValue),
    LessThanOrEqual(FieldValue),
}

#[derive(Clone)]
struct FilterResult {
    hits: RoaringBitmap,
}

impl FilterResult {
    fn and(self, another: FilterResult) -> FilterResult {
        FilterResult {
            hits: self.hits & another.hits,
        }
    }

    fn or(self, another: FilterResult) -> FilterResult {
        FilterResult {
            hits: self.hits | another.hits,
        }
    }

    fn read_matches(&self) -> Vec<u32> {
        self.hits.iter().collect()
    }
}

struct EntityStorage<T> {
    /// Indices available associated by data's field name
    indices: HashMap<String, Index>,

    /// Mapping between position of a data item in the index and its ID
    position_to_id: HashMap<u32, DataItemId>,

    /// Mapping between position of a data item in the index and its ID
    id_to_position: HashMap<DataItemId, u32>,

    /// Data available in the storage associated by the ID
    data: HashMap<DataItemId, T>,
}

impl<T: Indexable> EntityStorage<T> {
    fn new() -> Self {
        Self::default()
    }

    fn attach<I: IntoIterator<Item = T>>(&mut self, data: I) {
        for item in data {
            self.data.insert(item.id(), item);
        }
    }

    fn index(&mut self) {
        for (position, (id, item)) in self.data.iter().enumerate() {
            let position = position as u32;

            for property in item.index_values() {
                // Create index for the key value
                let index = self
                    .indices
                    .entry(property.name)
                    .or_insert(Index::from_type(&property.descriptor));

                index.append(property.value, position);
            }

            // Associate index position to the field ID
            self.position_to_id.insert(position, *id);
            self.id_to_position.insert(*id, position);
        }
    }

    fn get_id_by_position(&self, position: &u32) -> Option<&DataItemId> {
        self.position_to_id.get(position)
    }

    fn get_position_by_id(&self, id: &DataItemId) -> Option<&u32> {
        self.id_to_position.get(id)
    }
}

impl<T> Default for EntityStorage<T> {
    fn default() -> Self {
        EntityStorage {
            indices: Default::default(),
            position_to_id: Default::default(),
            id_to_position: Default::default(),
            data: Default::default(),
        }
    }
}

#[derive(PartialEq, Eq)]
struct DeltaScope {
    id: DataItemId,
    field_name: String,
}

// TODO: Build before and after value in the engine?
struct DeltaChange {
    scope: DeltaScope,
    before: Option<FieldValue>,
    after: Option<FieldValue>,
}

impl DeltaChange {
    fn new(id: DataItemId, field_name: String) -> Self {
        DeltaChange {
            scope: DeltaScope { id, field_name },
            before: None,
            after: None,
        }
    }

    fn before(mut self, before: FieldValue) -> Self {
        self.before = Some(before);
        self
    }

    fn after(mut self, after: FieldValue) -> Self {
        self.after = Some(after);
        self
    }
}

trait Delta {
    type Value;

    fn change(&self) -> &DeltaChange;

    fn apply_data(&self, value: &mut Self::Value);
}

type BoxedDelta<T> = Box<dyn Delta<Value = T>>;

struct Pagination {
    start: usize,
    size: usize,
}

impl Pagination {
    fn new(start: usize, size: usize) -> Self {
        Pagination { start, size }
    }
}

struct Sort {
    by: String,
}

impl Sort {
    fn new(by: String) -> Self {
        Sort { by }
    }

    fn apply(&self, items: &RoaringBitmap, indices: &HashMap<String, Index>) -> Vec<u32> {
        let index = indices
            .get(&self.by)
            .expect("Sort by criteria does not have an index.");

        index.sort(items)
    }
}

struct QueryExecution {
    filter: CompositeFilter,
    sort: Option<Sort>,
    pagination: Option<Pagination>,
}

impl QueryExecution {
    fn new(filter: CompositeFilter) -> Self {
        QueryExecution {
            filter,
            pagination: None,
            sort: None,
        }
    }

    fn with_sort(mut self, sort: Sort) -> Self {
        self.sort = Some(sort);
        self
    }

    fn with_pagination(mut self, pagination: Pagination) -> Self {
        self.pagination = Some(pagination);
        self
    }

    fn run<T>(self, storage: &EntityStorage<T>, deltas: &[BoxedDelta<T>]) -> Vec<T>
    where
        T: Indexable + Clone,
    {
        let indices = QueryExecution::read_indices(storage, deltas);

        let filter_result = self.filter.apply(&indices).unwrap();
        let item_ids = self.read_positions(filter_result, &indices, storage);

        QueryExecution::read_data(&item_ids, storage, deltas)
    }

    fn read_indices<T>(
        storage: &EntityStorage<T>,
        deltas: &[BoxedDelta<T>],
    ) -> HashMap<String, Index>
    where
        T: Indexable,
    {
        let mut indices = storage.indices.clone();

        for delta in deltas {
            let change = delta.change();

            let index = indices.get_mut(&change.scope.field_name);
            let position = storage.get_position_by_id(&change.scope.id);

            match (index, position) {
                (Some(index), Some(position)) => index.apply_change(*position, delta.change()),
                _ => continue,
            }
        }

        indices
    }

    fn read_positions<T>(
        &self,
        filter_result: FilterResult,
        indices: &HashMap<String, Index>,
        storage: &EntityStorage<T>,
    ) -> HashSet<DataItemId>
    where
        T: Indexable,
    {
        let sorted_items = self
            .sort
            .as_ref()
            .map(|sort| sort.apply(&filter_result.hits, indices))
            .unwrap_or_else(|| filter_result.hits.iter().collect());

        // TODO: Unify pagination?
        if let Some(pagination) = &self.pagination {
            return sorted_items
                .iter()
                .skip(pagination.start)
                .take(pagination.size)
                .flat_map(|position| storage.get_id_by_position(position))
                .copied()
                .collect();
        }

        sorted_items
            .iter()
            .flat_map(|position| storage.get_id_by_position(position))
            .copied()
            .collect()
    }

    fn read_data<T>(
        ids: &HashSet<DataItemId>,
        storage: &EntityStorage<T>,
        deltas: &[BoxedDelta<T>],
    ) -> Vec<T>
    where
        T: Clone,
    {
        let mut data = Vec::new();

        let deltas_by_id: HashMap<_, _> = deltas
            .iter()
            .map(|delta| (delta.change().scope.id, delta))
            .collect();

        for id in ids {
            if let Some(mut item) = storage.data.get(id).cloned() {
                if let Some(delta) = deltas_by_id.get(id) {
                    delta.apply_data(&mut item);
                }

                data.push(item);
            }
        }

        data
    }
}

struct Engine<T> {
    storage: EntityStorage<T>,
    deltas: Vec<BoxedDelta<T>>,
}

impl<T> Engine<T> {
    fn new(storage: EntityStorage<T>) -> Self {
        Engine {
            storage,
            deltas: Vec::new(),
        }
    }

    fn with_deltas<D>(mut self, deltas: Vec<D>) -> Self
    where
        D: Delta<Value = T> + 'static,
    {
        for delta in deltas {
            self.deltas.push(Box::new(delta));
        }
        self
    }
}

impl<T: Indexable + Clone> Engine<T> {
    fn query(&self, execution: QueryExecution) -> Vec<T> {
        execution.run(&self.storage, &self.deltas)
    }
}

#[cfg(test)]
mod tests {
    use crate::index::IndexableValue;
    use crate::{
        CompositeFilter, DataItemId, Delta, DeltaChange, Engine, EntityStorage, FieldValue,
        Indexable, Pagination, QueryExecution, Sort,
    };
    use lazy_static::lazy_static;
    use std::collections::HashSet;

    #[derive(Debug, PartialEq, Clone)]
    enum Sport {
        Basketball,
        Football,
    }

    impl Sport {
        fn as_string(&self) -> String {
            match self {
                Sport::Basketball => "basketball".to_string(),
                Sport::Football => "football".to_string(),
            }
        }
    }

    #[derive(Debug, PartialEq, Clone)]
    struct Player {
        id: usize,
        name: String,
        score: f64,
        sport: Sport,
    }

    impl Indexable for Player {
        fn id(&self) -> DataItemId {
            self.id
        }

        fn index_values(&self) -> Vec<IndexableValue> {
            vec![
                IndexableValue::string("name".to_string(), self.name.to_string()),
                IndexableValue::numeric("score".to_string(), self.score),
                IndexableValue::enumerate(
                    "sport".to_string(),
                    self.sport.as_string(),
                    HashSet::from_iter([
                        Sport::Basketball.as_string(),
                        Sport::Football.as_string(),
                    ]),
                ),
            ]
        }
    }

    struct DecreaseScoreDelta {
        change: DeltaChange,
    }

    impl DecreaseScoreDelta {
        fn new(id: DataItemId, score: f64) -> Self {
            DecreaseScoreDelta {
                change: DeltaChange::new(id, "score".to_string())
                    .before(FieldValue::numeric(score))
                    .after(FieldValue::numeric(score - 1.0)),
            }
        }
    }

    impl Delta for DecreaseScoreDelta {
        type Value = Player;

        fn change(&self) -> &DeltaChange {
            &self.change
        }

        fn apply_data(&self, value: &mut Self::Value) {
            value.score -= 1.0;
        }
    }

    struct SwitchSportsDelta {
        change: DeltaChange,
        new_sport: Sport,
    }

    impl SwitchSportsDelta {
        fn new(id: DataItemId, before: Sport, new_sport: Sport) -> Self {
            SwitchSportsDelta {
                change: DeltaChange::new(id, "sport".to_string())
                    .before(FieldValue::string(before.as_string()))
                    .after(FieldValue::string(new_sport.as_string())),
                new_sport,
            }
        }
    }

    impl Delta for SwitchSportsDelta {
        type Value = Player;

        fn change(&self) -> &DeltaChange {
            &self.change
        }

        fn apply_data(&self, value: &mut Self::Value) {
            value.sport = self.new_sport.clone();
        }
    }

    lazy_static! {
        static ref MICHAEL_JORDAN: Player = Player {
            id: 0,
            name: "Michael Jordan".to_string(),
            score: 10.0,
            sport: Sport::Basketball,
        };
        static ref LIONEL_MESSI: Player = Player {
            id: 1,
            name: "Lionel Messi".to_string(),
            score: 9.0,
            sport: Sport::Football,
        };
        static ref CRISTIANO_RONALDO: Player = Player {
            id: 2,
            name: "Cristiano Ronaldo".to_string(),
            score: 9.0,
            sport: Sport::Football,
        };
        static ref ROGER: Player = Player {
            id: 2,
            name: "Roger".to_string(),
            score: 5.0,
            sport: Sport::Football,
        };
    }

    fn create_random_players(count: usize) -> Vec<Player> {
        (0..count)
            .into_iter()
            .map(create_player_from_index)
            .collect()
    }

    fn create_player_from_index(index: usize) -> Player {
        Player {
            id: index,
            name: format!("Player {}", index),
            score: index as f64,
            sport: if index % 2 == 0 {
                Sport::Basketball
            } else {
                Sport::Football
            },
        }
    }

    fn storage(data: Vec<Player>) -> EntityStorage<Player> {
        let mut storage = EntityStorage::new();

        storage.attach(data);
        storage.index();

        storage
    }

    #[test]
    fn applies_enum_eq_filter() {
        // given
        let storage = storage(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            CRISTIANO_RONALDO.clone(),
        ]);
        let engine = Engine::new(storage);

        // when
        let execution = QueryExecution::new(CompositeFilter::eq(
            "sport",
            FieldValue::string("football".to_string()),
        ));
        let mut matches = engine.query(execution);

        // then
        matches.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(
            matches,
            vec![LIONEL_MESSI.clone(), CRISTIANO_RONALDO.clone()]
        );
    }

    #[test]
    fn applies_numeric_between_filter() {
        // given
        let storage = storage(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            ROGER.clone(),
        ]);
        let engine = Engine::new(storage);

        // when
        let execution = QueryExecution::new(CompositeFilter::between(
            "score",
            FieldValue::numeric(6.0),
            FieldValue::numeric(10.0),
        ));
        let mut matches = engine.query(execution);

        // then
        matches.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(matches, vec![MICHAEL_JORDAN.clone(), LIONEL_MESSI.clone()]);
    }

    #[test]
    fn applies_numeric_ge_filter() {
        // given
        let storage = storage(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            ROGER.clone(),
        ]);
        let engine = Engine::new(storage);

        // when
        let execution = QueryExecution::new(CompositeFilter::ge("score", FieldValue::numeric(6.0)));
        let mut matches = engine.query(execution);

        // then
        matches.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(matches, vec![MICHAEL_JORDAN.clone(), LIONEL_MESSI.clone()]);
    }

    #[test]
    fn applies_numeric_le_filter() {
        // given
        let storage = storage(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            ROGER.clone(),
        ]);
        let engine = Engine::new(storage);

        // when
        let execution = QueryExecution::new(CompositeFilter::le("score", FieldValue::numeric(6.0)));
        let mut matches = engine.query(execution);

        // then
        matches.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(matches, vec![ROGER.clone()]);
    }

    #[test]
    fn applies_numeric_delta() {
        // given
        let storage = storage(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            CRISTIANO_RONALDO.clone(),
        ]);
        let deltas = vec![
            DecreaseScoreDelta::new(MICHAEL_JORDAN.id, MICHAEL_JORDAN.score),
            DecreaseScoreDelta::new(LIONEL_MESSI.id, LIONEL_MESSI.score),
        ];

        let engine = Engine::new(storage).with_deltas(deltas);

        // when
        let execution = QueryExecution::new(CompositeFilter::eq(
            "sport",
            FieldValue::string("football".to_string()),
        ));
        let mut matches = engine.query(execution);

        // then
        matches.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(
            matches,
            vec![
                Player {
                    id: LIONEL_MESSI.id,
                    name: LIONEL_MESSI.name.to_string(),
                    score: 8.0,
                    sport: LIONEL_MESSI.sport.clone(),
                },
                CRISTIANO_RONALDO.clone()
            ]
        );
    }

    #[test]
    fn applies_enum_delta() {
        // given
        let storage = storage(vec![
            MICHAEL_JORDAN.clone(),
            LIONEL_MESSI.clone(),
            CRISTIANO_RONALDO.clone(),
        ]);
        let deltas = vec![SwitchSportsDelta::new(
            MICHAEL_JORDAN.id,
            MICHAEL_JORDAN.sport.clone(),
            Sport::Football,
        )];

        let engine = Engine::new(storage).with_deltas(deltas);

        // when
        let execution = QueryExecution::new(CompositeFilter::eq(
            "sport",
            FieldValue::string("football".to_string()),
        ));
        let mut matches = engine.query(execution);

        // then
        matches.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(
            matches,
            vec![
                Player {
                    id: MICHAEL_JORDAN.id,
                    name: MICHAEL_JORDAN.name.to_string(),
                    score: MICHAEL_JORDAN.score,
                    sport: Sport::Football,
                },
                LIONEL_MESSI.clone(),
                CRISTIANO_RONALDO.clone()
            ]
        );
    }

    #[test]
    fn applies_pagination() {
        // given
        let storage = storage(create_random_players(20));
        let engine = Engine::new(storage);

        let filter = CompositeFilter::eq("sport", FieldValue::string("football".to_string()));
        let pagination = Pagination::new(2, 5);

        // when
        let execution = QueryExecution::new(filter)
            .with_sort(Sort::new("score".to_string()))
            .with_pagination(pagination);

        let mut matches = engine.query(execution);

        // then
        matches.sort_by(|a, b| a.id.cmp(&b.id));

        assert_eq!(
            matches,
            vec![
                create_player_from_index(5),
                create_player_from_index(7),
                create_player_from_index(9),
                create_player_from_index(11),
                create_player_from_index(13)
            ]
        );
    }
}
