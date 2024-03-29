use std::collections::HashMap;

use bimap::BiHashMap;
use roaring::RoaringBitmap;

use crate::DataItemId;
use crate::index::{Index, Indexable};

pub struct InMemoryStorage<T> {
    /// Indices available for the given associated data
    pub(crate) indices: EntityIndices,

    /// Mapping between position of a data item in the index and its ID
    position_id: BiHashMap<u32, DataItemId>,

    /// Data available in the storage associated by the ID
    pub(crate) data: HashMap<DataItemId, T>,
}

impl<T: Indexable> InMemoryStorage<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn carry<I: IntoIterator<Item=T>>(&mut self, data: I) {
        self.clear();
        for item in data {
            self.add(item);
        }
    }

    pub(crate) fn clear(&mut self) {
        self.indices.all.clear();
        self.indices.field_indices.clear();
        self.position_id.clear();
        self.data.clear();
    }

    pub(crate) fn add(&mut self, item: T) {
        let id = item.id();

        let position = self
            .position_id
            .get_by_right(&id)
            .copied()
            .unwrap_or(self.position_id.len() as u32);

        for index_value in item.index_values() {
            // Create index for the key value
            let index = self
                .indices
                .field_indices
                .entry(index_value.name)
                .or_insert(Index::from_type(&index_value.descriptor));

            index.put(index_value.value, position);
        }
        self.indices.all.insert(position);

        // Associate index position to the field ID
        self.data.insert(id, item);
        self.position_id.insert(position, id);
    }

    pub(crate) fn remove(&mut self, id: &DataItemId) {
        if let Some((position, _)) = self.position_id.remove_by_right(id) {
            self.data.remove(id);

            // Remove item from indices
            for index in self.indices.field_indices.values_mut() {
                index.remove_item(position);
            }
            self.indices.all.remove(position);
        }
    }

    pub(crate) fn get_id_by_position(&self, position: &u32) -> Option<&DataItemId> {
        self.position_id.get_by_left(position)
    }

    pub(crate) fn get_position_by_id(&self, id: &DataItemId) -> Option<&u32> {
        self.position_id.get_by_right(id)
    }

    fn read_indices(&self, fields: &[String]) -> EntityIndices {
        let field_indices = fields
            .iter()
            .filter_map(|name| {
                self.indices
                    .field_indices
                    .get(name)
                    .cloned()
                    .map(|index| (name.to_string(), index))
            })
            .collect();

        EntityIndices {
            field_indices,
            all: self.indices.all.clone(),
        }
    }

    fn read_all_indices(&self) -> EntityIndices {
        let field_indices = self
            .indices
            .field_indices
            .iter()
            .map(|(name, index)| (name.to_string(), index.clone()))
            .collect();

        EntityIndices {
            field_indices,
            all: self.indices.all.clone(),
        }
    }
}

impl<T: Clone> InMemoryStorage<T> {
    fn read_by_id(&self, id: &DataItemId) -> Option<T> {
        self.data.get(id).cloned()
    }
}

impl<T> Default for InMemoryStorage<T> {
    fn default() -> Self {
        InMemoryStorage {
            indices: Default::default(),
            position_id: Default::default(),
            data: Default::default(),
        }
    }
}

#[derive(Default)]
pub struct EntityIndices {
    /// Indices available associated by data's field name
    pub(crate) field_indices: HashMap<String, Index>,

    /// Bitmap including all items' positions
    pub(crate) all: RoaringBitmap,
}
