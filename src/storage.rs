use std::collections::HashMap;
use std::path::Path;

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

const ALL_ITEMS_KEY: &str = "__all";

pub enum EntityStorage<T> {
    Disk(DiskStorage<T>),
    InMemory(InMemoryStorage<T>),
}

impl<T: Indexable + Serialize> EntityStorage<T> {
    pub fn carry(&mut self, data: Vec<T>) {
        match self {
            EntityStorage::Disk(disk) => disk.carry(data),
            EntityStorage::InMemory(in_memory) => in_memory.carry(data),
        }
    }

    pub fn clear(&mut self) {
        match self {
            EntityStorage::Disk(disk) => disk.clear(),
            EntityStorage::InMemory(in_memory) => in_memory.clear(),
        }
    }

    pub fn add(&mut self, item: T) {
        match self {
            EntityStorage::Disk(disk) => disk.add(&[item]),
            EntityStorage::InMemory(in_memory) => in_memory.add(item),
        }
    }

    pub fn remove(&mut self, id: &DataItemId) {
        match self {
            EntityStorage::Disk(disk) => disk.remove(std::slice::from_ref(id)),
            EntityStorage::InMemory(in_memory) => in_memory.remove(id),
        }
    }

    pub fn read_indices(&self, fields: &[String]) -> EntityIndices {
        match self {
            EntityStorage::Disk(disk) => disk.read_indices(fields),
            EntityStorage::InMemory(in_memory) => in_memory.read_indices(fields),
        }
    }

    pub fn read_all_indices(&self) -> EntityIndices {
        match self {
            EntityStorage::Disk(disk) => disk.read_all_indices(),
            EntityStorage::InMemory(in_memory) => in_memory.read_all_indices(),
        }
    }
}

impl<T: Clone + for<'a> Deserialize<'a>> EntityStorage<T> {
    pub fn read(&self, id: &DataItemId) -> Option<T> {
        match self {
            EntityStorage::Disk(disk) => disk.read_by_id(id),
            EntityStorage::InMemory(in_memory) => in_memory.read_by_id(id),
        }
    }
}

pub(crate) fn position_to_id(position: u32) -> DataItemId {
    usize::try_from(position).expect("Position could not be mapped into an item ID")
}

pub(crate) fn id_to_position(id: DataItemId) -> u32 {
    u32::try_from(id).expect("ID could not be mapped into an index position")
}

pub struct StorageBuilder {
    name: Option<String>,
    kind: StorageKind,
}

impl StorageBuilder {
    pub fn disk(name: &str) -> Self {
        StorageBuilder {
            name: Some(name.into()),
            kind: StorageKind::Disk,
        }
    }

    pub fn in_memory() -> Self {
        StorageBuilder {
            name: None,
            kind: StorageKind::InMemory,
        }
    }

    pub fn build<T: Indexable + 'static>(&self) -> EntityStorage<T> {
        match self.kind {
            StorageKind::Disk => {
                let name = self
                    .name
                    .as_ref()
                    .expect("You must specify a name for your entity to be stored in disk.");

                EntityStorage::Disk(DiskStorage::init(name))
            }
            StorageKind::InMemory => EntityStorage::InMemory(InMemoryStorage::new()),
        }
    }
}

pub enum StorageKind {
    Disk,
    InMemory,
}

pub struct DiskStorage<T> {
    env: Env,
    data: Database<OwnedType<DataItemId>, SerdeBincode<T>>,
    indices: Database<Str, SerdeBincode<Index>>,
    documents: Database<Str, SerdeBincode<RoaringBitmap>>,
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

        DiskStorage {
            env,
            indices,
            documents,
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
            let position = id_to_position(id);

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
            let position = id_to_position(*id);

            // Remove item from data and ID to position mapping
            let present = self.data.delete(&mut txn, id).unwrap();
            if !present {
                return;
            }

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

        txn.commit().unwrap();
    }

    fn get_all_positions(&self, txn: &RoTxn) -> Option<RoaringBitmap> {
        self.documents
            .get(txn, ALL_ITEMS_KEY)
            .expect("Could not read all items from the DB.")
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
        self.data.clear();
    }

    pub(crate) fn add(&mut self, item: T) {
        let id = item.id();

        let position = id_to_position(id);

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
    }

    pub(crate) fn remove(&mut self, id: &DataItemId) {
        if self.data.remove(id).is_none() {
            return;
        }

        let position = id_to_position(*id);

        // Remove item from indices
        for index in self.indices.field_indices.values_mut() {
            index.remove_item(position);
        }
        self.indices.all.remove(position);
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
