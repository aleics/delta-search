use std::collections::HashMap;
use std::path::Path;

use bimap::BiHashMap;
use heed::{Database, Env, EnvOpenOptions, RoTxn};
use heed::types::*;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};

use crate::DataItemId;
use crate::index::{Index, Indexable};

const FOLDER: &str = "./delta-db";
const DATA_DB_NAME: &str = "data";
const INDICES_DB_NAME: &str = "indices";
const DOCUMENTS_DB_NAME: &str = "documents";
const POSITION_TO_ID_DB_NAME: &str = "position_to_id";
const ID_TO_POSITION_DB_NAME: &str = "id_to_position";

const ALL_ITEMS_KEY: &str = "__all";

pub struct DiskStorage<T> {
    env: Env,
    data: Database<OwnedType<DataItemId>, SerdeBincode<T>>,
    indices: Database<Str, SerdeBincode<Index>>,
    documents: Database<Str, SerdeBincode<RoaringBitmap>>,
    id_to_position: Database<OwnedType<DataItemId>, OwnedType<u32>>,
    position_to_id: Database<OwnedType<u32>, OwnedType<DataItemId>>,
}

impl<T: 'static> DiskStorage<T> {
    pub fn init(name: &str) -> Self {
        let file_name = format!("{}.mdb", name);
        let path = Path::new(FOLDER).join(file_name);

        std::fs::create_dir_all(&path).unwrap();

        let env = EnvOpenOptions::new()
            .map_size(100 * 1024 * 1024) // 100 MB max size
            .max_dbs(3000)
            .open(path)
            .unwrap();

        let data: Database<OwnedType<DataItemId>, SerdeBincode<T>> =
            env.create_database(Some(DATA_DB_NAME)).unwrap();
        let indices: Database<Str, SerdeBincode<Index>> =
            env.create_database(Some(INDICES_DB_NAME)).unwrap();
        let documents: Database<Str, SerdeBincode<RoaringBitmap>> =
            env.create_database(Some(DOCUMENTS_DB_NAME)).unwrap();
        let id_to_position: Database<OwnedType<DataItemId>, OwnedType<u32>> =
            env.create_database(Some(ID_TO_POSITION_DB_NAME)).unwrap();
        let position_to_id: Database<OwnedType<u32>, OwnedType<DataItemId>> =
            env.create_database(Some(POSITION_TO_ID_DB_NAME)).unwrap();

        DiskStorage {
            env,
            indices,
            documents,
            position_to_id,
            id_to_position,
            data,
        }
    }
}

impl<T: Indexable + Serialize> DiskStorage<T> {
    pub fn carry<I>(&self, data: I)
        where
            I: IntoIterator<Item=T>,
    {
        self.clear();

        let mut chunks = data.into_iter().array_chunks::<100>();

        for chunk in chunks.by_ref() {
            self.add(&chunk);
        }

        if let Some(remainder) = chunks.into_remainder() {
            self.add(remainder.as_slice());
        }
    }

    pub(crate) fn clear(&self) {
        let mut txn = self.env.write_txn().unwrap();

        self.indices
            .clear(&mut txn)
            .expect("Could not clear indices.");
        self.position_to_id
            .clear(&mut txn)
            .expect("Could not clear position to IDs mapping.");
        self.id_to_position
            .clear(&mut txn)
            .expect("Could not clear ID to positions mapping.");
        self.data.clear(&mut txn).expect("Could not clear data.");

        txn.commit().unwrap();
    }

    pub(crate) fn add(&self, items: &[T]) {
        let mut txn = self.env.write_txn().unwrap();

        let mut all = self.get_all_positions(&txn).unwrap_or_default();
        let mut indices_to_store: HashMap<String, Index> = HashMap::new();

        for item in items {
            // Read item ID and determine position
            let id = item.id();

            let position = self
                .get_position_by_id(&id)
                .unwrap_or_else(|| self.id_to_position.len(&txn).unwrap() as u32);

            // Insert item in the data DB
            self.data.put(&mut txn, &id, item).unwrap();

            // Update indices with item's indexed values
            for index_value in item.index_values() {
                let value = index_value.value.clone();

                if let Some(index) = indices_to_store.get_mut(&index_value.name) {
                    index.put(value, position);
                } else {
                    let mut index = self
                        .indices
                        .get(&txn, &index_value.name)
                        .unwrap()
                        .unwrap_or_else(|| Index::from_type(&index_value.descriptor));

                    index.put(value, position);
                    indices_to_store.insert(index_value.name, index);
                }
            }


            all.insert(position);

            // Add item in the position to ID mapping
            self.position_to_id.put(&mut txn, &position, &id).unwrap();
            self.id_to_position.put(&mut txn, &id, &position).unwrap();
        }

        self.documents.put(&mut txn, ALL_ITEMS_KEY, &all).unwrap();

        for (name, index) in indices_to_store {
            self.indices.put(&mut txn, &name, &index).unwrap();
        }

        txn.commit().unwrap();
    }

    fn remove(&self, ids: &[DataItemId]) {
        let mut txn = self.env.write_txn().unwrap();

        for id in ids {
            if let Some(position) = self.get_position_by_id(id) {

                // Remove item from data and ID to position mapping
                self.data.delete(&mut txn, id).unwrap();
                self.id_to_position.delete(&mut txn, id).unwrap();
                self.position_to_id.delete(&mut txn, &position).unwrap();

                let mut entries = self
                    .indices
                    .iter_mut(&mut txn)
                    .expect("Could not iterate indices from the DB.");

                while let Some(entry) = entries.next() {
                    let (key, mut value) = entry
                        .map(|(key, value)| (key.to_string(), value))
                        .expect("Could not read entry while iterating indices from the DB.");

                    value.remove_item(position);
                    entries.put_current(&key, &value).unwrap();
                }

                drop(entries);

                if let Some(mut all) = self.get_all_positions(&txn) {
                    all.remove(position);
                    self.documents.put(&mut txn, ALL_ITEMS_KEY, &all).unwrap();
                }
            }
        }

        txn.commit().unwrap();
    }

    fn get_all_positions(&self, txn: &RoTxn) -> Option<RoaringBitmap> {
        self
            .documents
            .get(txn, ALL_ITEMS_KEY)
            .expect("Could not read all items from the DB.")
    }

    fn get_id_by_position(&self, position: &u32) -> Option<DataItemId> {
        let txn = self.env.read_txn().unwrap();

        self.position_to_id
            .get(&txn, position)
            .expect("Could not read id by position")
    }

    fn get_position_by_id(&self, id: &DataItemId) -> Option<u32> {
        let txn = self.env.read_txn().unwrap();

        self.id_to_position
            .get(&txn, id)
            .expect("Could not read position by id")
    }

    fn read_indices(&self, fields: &[String]) -> EntityIndices {
        let txn = self.env.read_txn().unwrap();

        let field_indices = fields
            .iter()
            .filter_map(|name| {
                let index = self.indices.get(&txn, name).unwrap();
                index.map(|index| (name.to_string(), index))
            })
            .collect();

        let all = self
            .documents
            .get(&txn, ALL_ITEMS_KEY)
            .unwrap()
            .unwrap_or_default();

        EntityIndices { field_indices, all }
    }

    fn read_all_indices(&self) -> EntityIndices {
        let txn = self.env.read_txn().unwrap();

        let field_indices = self
            .indices
            .iter(&txn)
            .expect("Could not iterate indices from the DB.")
            .map(|item| {
                item.map(|(key, value)| (key.into(), value))
                    .expect("Could not read index from DB.")
            })
            .collect();

        let all = self
            .documents
            .get(&txn, ALL_ITEMS_KEY)
            .expect("Could not read ALL items index from DB.")
            .expect("ALL items index is not present in DB");

        EntityIndices { field_indices, all }
    }
}

impl<T: Clone + for<'a> Deserialize<'a>> DiskStorage<T> {
    fn read_by_id(&self, id: &DataItemId) -> Option<T> {
        let txn = self.env.read_txn().unwrap();

        self.data
            .get(&txn, id)
            .expect("Could not read item from DB")
    }
}

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
