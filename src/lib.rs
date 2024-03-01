pub mod index;

use bimap::BiHashMap;
use ordered_float::OrderedFloat;
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};

use crate::index::Indexable;
use index::Index;
use roaring::RoaringBitmap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldValue {
    String(String),
    Numeric(OrderedFloat<f64>),
}

impl FieldValue {
    pub fn string(value: String) -> FieldValue {
        FieldValue::String(value)
    }

    pub fn numeric(value: f64) -> FieldValue {
        FieldValue::Numeric(OrderedFloat(value))
    }

    fn as_numeric(&self) -> Option<&OrderedFloat<f64>> {
        if let FieldValue::Numeric(value) = self {
            Some(value)
        } else {
            None
        }
    }

    fn get_numeric(self) -> Option<OrderedFloat<f64>> {
        if let FieldValue::Numeric(value) = self {
            Some(value)
        } else {
            None
        }
    }

    fn as_string(&self) -> Option<&String> {
        if let FieldValue::String(value) = self {
            Some(value)
        } else {
            None
        }
    }

    fn get_string(self) -> Option<String> {
        if let FieldValue::String(value) = self {
            Some(value)
        } else {
            None
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

pub type DataItemId = usize;

#[derive(Debug)]
pub enum CompositeFilter {
    And(Vec<CompositeFilter>),
    Or(Vec<CompositeFilter>),
    Single(Filter),
}

impl CompositeFilter {
    pub fn eq(name: &str, value: FieldValue) -> Self {
        CompositeFilter::Single(Filter {
            name: name.to_string(),
            operation: FilterOperation::Eq(value),
        })
    }

    pub fn between(name: &str, first: FieldValue, second: FieldValue) -> Self {
        CompositeFilter::Single(Filter {
            name: name.to_string(),
            operation: FilterOperation::Between(first, second),
        })
    }

    pub fn gt(name: &str, value: FieldValue) -> Self {
        CompositeFilter::Single(Filter {
            name: name.to_string(),
            operation: FilterOperation::GreaterThan(value),
        })
    }

    pub fn ge(name: &str, value: FieldValue) -> Self {
        CompositeFilter::Single(Filter {
            name: name.to_string(),
            operation: FilterOperation::GreaterOrEqual(value),
        })
    }

    pub fn lt(name: &str, value: FieldValue) -> Self {
        CompositeFilter::Single(Filter {
            name: name.to_string(),
            operation: FilterOperation::LessThan(value),
        })
    }

    pub fn le(name: &str, value: FieldValue) -> Self {
        CompositeFilter::Single(Filter {
            name: name.to_string(),
            operation: FilterOperation::LessThanOrEqual(value),
        })
    }

    fn apply(&self, indices: &QueryIndices) -> FilterResult {
        match self {
            CompositeFilter::And(filters) => {
                let result: Option<FilterResult> = filters.iter().fold(None, |acc, filter| {
                    acc.map(|current| current.and(filter.apply(indices)))
                });

                result.unwrap_or_else(FilterResult::empty)
            }
            CompositeFilter::Or(filters) => {
                let result: Option<FilterResult> = filters.iter().fold(None, |acc, filter| {
                    acc.map(|current| current.or(filter.apply(indices)))
                });

                result.unwrap_or_else(FilterResult::empty)
            }
            CompositeFilter::Single(filter) => {
                let index = indices.get(&filter.name).unwrap_or_else(|| {
                    panic!("Filter with name {} has no index assigned", &filter.name)
                });

                index.filter(&filter.operation)
            }
        }
    }
}

#[derive(Debug)]
pub struct Filter {
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
    fn new(hits: RoaringBitmap) -> Self {
        FilterResult { hits }
    }

    fn empty() -> Self {
        FilterResult {
            hits: RoaringBitmap::new(),
        }
    }

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
}

pub struct EntityStorage<T> {
    /// Indices available associated by data's field name
    indices: HashMap<String, Index>,

    /// Mapping between position of a data item in the index and its ID
    position_id: BiHashMap<u32, DataItemId>,

    /// Data available in the storage associated by the ID
    data: HashMap<DataItemId, T>,
}

impl<T: Indexable> EntityStorage<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn attach<I: IntoIterator<Item = T>>(&mut self, data: I) {
        for item in data {
            self.data.insert(item.id(), item);
        }
    }

    pub fn index(&mut self) {
        for (position, (id, item)) in self.data.iter().enumerate() {
            let position = position as u32;

            for property in item.index_values() {
                // Create index for the key value
                let index = self
                    .indices
                    .entry(property.name)
                    .or_insert(Index::from_type(&property.descriptor));

                index.put(property.value, position);
            }

            // Associate index position to the field ID
            self.position_id.insert(position, *id);
        }
    }

    fn get_id_by_position(&self, position: &u32) -> Option<&DataItemId> {
        self.position_id.get_by_left(position)
    }

    fn get_position_by_id(&self, id: &DataItemId) -> Option<&u32> {
        self.position_id.get_by_right(id)
    }
}

impl<T> Default for EntityStorage<T> {
    fn default() -> Self {
        EntityStorage {
            indices: Default::default(),
            position_id: Default::default(),
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
pub struct DeltaChange {
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

pub trait Delta {
    type Value;

    fn change(&self) -> DeltaChange;

    fn apply_data(&self, value: &mut Self::Value);
}

type BoxedDelta<T> = Box<dyn Delta<Value = T>>;

pub struct Pagination {
    start: usize,
    size: usize,
}

impl Pagination {
    pub fn new(start: usize, size: usize) -> Self {
        Pagination { start, size }
    }
}

pub struct Sort {
    by: String,
}

impl Sort {
    pub fn new(by: String) -> Self {
        Sort { by }
    }

    fn apply(&self, items: &RoaringBitmap, indices: &QueryIndices) -> Vec<u32> {
        let index = indices
            .get(&self.by)
            .expect("Sort by criteria does not have an index.");

        index.sort(items)
    }
}

struct QueryIndices<'a> {
    stored: &'a HashMap<String, Index>,
    affected: HashSet<DataItemId>,
    deltas: HashMap<String, Index>,
}

impl<'a> QueryIndices<'a> {
    fn new(stored: &'a HashMap<String, Index>) -> Self {
        QueryIndices {
            stored,
            affected: HashSet::new(),
            deltas: HashMap::new(),
        }
    }

    fn attach_deltas<T>(mut self, deltas: &[BoxedDelta<T>], storage: &EntityStorage<T>) -> Self
    where
        T: Indexable,
    {
        for delta in deltas {
            let change = delta.change();

            if let Some(current) = self.stored.get(&change.scope.field_name) {
                if let Some(position) = storage.get_position_by_id(&change.scope.id) {
                    // TODO: it's a bit sad that we need to clone the whole index to mutate only single positions.
                    let mut dynamic = current.clone();

                    if let Some(before) = change.before.as_ref() {
                        dynamic.remove(before, *position);
                    }

                    if let Some(after) = change.after {
                        dynamic.put(after, *position);
                    }

                    self.deltas.insert(change.scope.field_name, dynamic);
                    self.affected.insert(change.scope.id);
                }
            }
        }
        self
    }

    fn get(&self, name: &String) -> Option<&Index> {
        self.deltas.get(name).or_else(|| self.stored.get(name))
    }
}

pub struct QueryExecution {
    filter: CompositeFilter,
    sort: Option<Sort>,
    pagination: Option<Pagination>,
}

impl QueryExecution {
    pub fn new(filter: CompositeFilter) -> Self {
        QueryExecution {
            filter,
            pagination: None,
            sort: None,
        }
    }

    pub fn with_sort(mut self, sort: Sort) -> Self {
        self.sort = Some(sort);
        self
    }

    pub fn with_pagination(mut self, pagination: Pagination) -> Self {
        self.pagination = Some(pagination);
        self
    }

    pub fn run<T>(self, storage: &EntityStorage<T>, deltas: &[BoxedDelta<T>]) -> Vec<T>
    where
        T: Indexable + Clone,
    {
        let indices = QueryIndices::new(&storage.indices).attach_deltas(deltas, storage);

        let filter_result = self.filter.apply(&indices);
        let item_ids = self.read_positions(filter_result, &indices, storage);

        QueryExecution::read_data(&item_ids, storage, deltas)
    }

    fn read_positions<T>(
        &self,
        filter_result: FilterResult,
        indices: &QueryIndices,
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

pub struct Engine<T> {
    storage: EntityStorage<T>,
    deltas: Vec<BoxedDelta<T>>,
}

impl<T> Engine<T> {
    pub fn new(storage: EntityStorage<T>) -> Self {
        Engine {
            storage,
            deltas: Vec::new(),
        }
    }

    pub fn with_deltas<D>(mut self, deltas: Vec<D>) -> Self
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
    pub fn query(&self, execution: QueryExecution) -> Vec<T> {
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
        id: DataItemId,
        score: f64,
    }

    impl DecreaseScoreDelta {
        fn new(id: DataItemId, score: f64) -> Self {
            DecreaseScoreDelta { id, score }
        }
    }

    impl Delta for DecreaseScoreDelta {
        type Value = Player;

        fn change(&self) -> DeltaChange {
            DeltaChange::new(self.id, "score".to_string())
                .before(FieldValue::numeric(self.score))
                .after(FieldValue::numeric(self.score - 1.0))
        }

        fn apply_data(&self, value: &mut Self::Value) {
            value.score -= 1.0;
        }
    }

    struct SwitchSportsDelta {
        id: DataItemId,
        current: Sport,
        new_sport: Sport,
    }

    impl SwitchSportsDelta {
        fn new(id: DataItemId, current: Sport, new_sport: Sport) -> Self {
            SwitchSportsDelta {
                id,
                current,
                new_sport,
            }
        }
    }

    impl Delta for SwitchSportsDelta {
        type Value = Player;

        fn change(&self) -> DeltaChange {
            DeltaChange::new(self.id, "sport".to_string())
                .before(FieldValue::string(self.current.as_string()))
                .after(FieldValue::string(self.new_sport.as_string()))
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
        (0..count).map(create_player_from_index).collect()
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
