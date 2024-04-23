use std::collections::HashMap;
use std::path::Path;

use heed::byteorder::BE;
use heed::types::*;
use heed::{Database, Env, EnvOpenOptions, RoTxn};
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use time::Date;

use crate::data::{date_to_timestamp, DataItem};
use crate::index::{Index, TypeDescriptor};
use crate::query::DeltaChange;
use crate::DataItemId;

pub(crate) const DB_FOLDER: &str = "./delta-db";
const DATA_DB_NAME: &str = "data";
const INDICES_DB_NAME: &str = "indices";
const DOCUMENTS_DB_NAME: &str = "documents";
const DELTAS_DB_NAME: &str = "deltas";

const ALL_ITEMS_KEY: &str = "__all";

pub(crate) fn position_to_id(position: u32) -> DataItemId {
    usize::try_from(position).expect("Position could not be mapped into an item ID")
}

pub(crate) fn id_to_position(id: DataItemId) -> u32 {
    u32::try_from(id).expect("ID could not be mapped into an index position")
}

pub(crate) fn read_stored_entity_names() -> Vec<String> {
    let mut names = Vec::new();
    let Ok(paths) = std::fs::read_dir(DB_FOLDER) else {
        return names;
    };

    for path in paths {
        let path_buf = path.unwrap().path();
        let path = path_buf.as_path();

        let is_mdb = path
            .extension()
            .map(|extension| extension == "mdb")
            .unwrap_or(false);

        if is_mdb {
            if let Some(name) = path.file_stem().and_then(|name| name.to_str()) {
                names.push(name.to_string())
            }
        }
    }

    names
}

pub struct StorageBuilder {
    name: Option<String>,
}

impl StorageBuilder {
    pub fn new(name: &str) -> Self {
        StorageBuilder {
            name: Some(name.into()),
        }
    }

    pub fn build(&self) -> EntityStorage {
        let name = self
            .name
            .as_ref()
            .expect("You must specify a name for your entity to be stored in disk.");

        EntityStorage::init(name)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StoredDelta {
    before: Index,
    after: Index,
}

impl StoredDelta {
    fn from_type(descriptor: &TypeDescriptor) -> Self {
        StoredDelta {
            before: Index::from_type(descriptor),
            after: Index::from_type(descriptor),
        }
    }
}

/// Storage in disk using `LMDB` for the data and their related indices.
pub struct EntityStorage {
    pub(crate) id: String,
    env: Env,
    data: Database<OwnedType<DataItemId>, SerdeBincode<DataItem>>,
    indices: Database<Str, SerdeBincode<Index>>,
    documents: Database<Str, SerdeBincode<RoaringBitmap>>,
    deltas: Database<OwnedType<I64<BE>>, SerdeBincode<HashMap<String, StoredDelta>>>,
    index_descriptors: HashMap<String, TypeDescriptor>,
}

impl EntityStorage {
    /// Initialises a new `DiskStorage` instance by creating the necessary files
    /// and LMDB `Database` entries.
    pub fn init(name: &str) -> Self {
        let file_name = format!("{}.mdb", name);
        let path = Path::new(DB_FOLDER).join(file_name);

        std::fs::create_dir_all(&path).unwrap();

        let env = EnvOpenOptions::new()
            .map_size(100 * 1024 * 1024) // 100 MB max size
            .max_dbs(3000)
            .open(path)
            .unwrap();

        let data = env.create_database(Some(DATA_DB_NAME)).unwrap_or_else(|_| {
            panic!(
                "Could not create database for storing data in entity {}",
                name
            )
        });
        let indices = env
            .create_database(Some(INDICES_DB_NAME))
            .unwrap_or_else(|_| {
                panic!(
                    "Could not create database for storing indices in entity {}",
                    name
                )
            });
        let documents = env
            .create_database(Some(DOCUMENTS_DB_NAME))
            .unwrap_or_else(|_| {
                panic!(
                    "Could not create database for storing documents in entity {}",
                    name
                )
            });

        let deltas = env
            .create_database(Some(DELTAS_DB_NAME))
            .unwrap_or_else(|_| {
                panic!(
                    "Could not create database for storing deltas in entity {}",
                    name
                )
            });

        let mut storage = EntityStorage {
            id: name.to_string(),
            env,
            indices,
            documents,
            data,
            deltas,
            index_descriptors: HashMap::new(),
        };

        storage.propagate_indices();

        storage
    }

    /// Propagate the index data into other in-memory data used for faster access to certain
    /// properties and reduce deserialization overhead while running certain operations.
    fn propagate_indices(&mut self) {
        let txn = self.env.read_txn().unwrap();

        let entries = self
            .indices
            .iter(&txn)
            .expect("Could not read indices while creating cache")
            .map(|entry| entry.expect("Could not read index entry while creating cache"));

        for (name, index) in entries {
            self.index_descriptors
                .insert(name.to_string(), index.create_descriptor());
        }
    }

    /// Get the current entity's storage path.
    pub(crate) fn get_path(&self) -> &Path {
        self.env.path()
    }

    /// Fill the DB with data by clearing the previous one. This is meant for when initialising
    /// the storage and remove any previous data.
    pub fn carry<I>(&mut self, data: I)
    where
        I: IntoIterator<Item = DataItem>,
    {
        self.clear();
        self.add_multiple(data);
    }

    /// Add multiple items using chunks so that multiple transactions are commited, depending on
    /// the amount of chunks generated.
    fn add_multiple<I>(&self, data: I)
    where
        I: IntoIterator<Item = DataItem>,
    {
        // Add elements in chunks to optimise the storing execution write operations in bulk.
        let mut chunks = data.into_iter().array_chunks::<100>();
        for chunk in chunks.by_ref() {
            self.add(&chunk);
        }

        // In case there's some leftovers after splitting in chunks
        if let Some(remainder) = chunks.into_remainder() {
            self.add(remainder.as_slice());
        }
    }

    /// Clears the current storage indices and data.
    pub fn clear(&mut self) {
        let mut txn = self.env.write_txn().unwrap();

        self.data
            .clear(&mut txn)
            .expect("Could not clear data_items");
        self.indices
            .clear(&mut txn)
            .expect("Could not clear indices");
        self.documents
            .clear(&mut txn)
            .expect("Could not clear documents");
        self.index_descriptors.clear();

        txn.commit().unwrap();
    }

    /// Store an amount of items in the database using a single transaction.
    /// Any index is as well updated with the stored items after the transaction
    /// is committed.
    pub fn add(&self, items: &[DataItem]) {
        let mut txn = self.env.write_txn().unwrap();

        let mut indices_to_store: HashMap<String, Index> = HashMap::new();
        let mut all = self.get_all_positions(&txn).unwrap_or_default();

        for item in items {
            // Read item ID and determine position
            let position = id_to_position(item.id);

            // Insert item in the data DB
            self.data.put(&mut txn, &item.id, item).unwrap();

            // Update indices in memory with the item data to reduce (de)serialization overhead
            // if we update index one by one in the DB.
            for (index_name, index_descriptor) in &self.index_descriptors {
                let Some(value) = item.fields.get(index_name).cloned() else {
                    continue;
                };

                if let Some(index) = indices_to_store.get_mut(index_name) {
                    index.put(value, position);
                } else {
                    let mut index = self
                        .indices
                        .get(&txn, index_name)
                        .unwrap()
                        .unwrap_or_else(|| Index::from_type(index_descriptor));

                    index.put(value, position);
                    indices_to_store.insert(index_name.clone(), index);
                }
            }

            all.insert(position);
        }

        // Store indices in the DB for each index that has been changed.
        self.documents.put(&mut txn, ALL_ITEMS_KEY, &all).unwrap();

        for (name, index) in indices_to_store {
            self.indices.put(&mut txn, &name, &index).unwrap();
        }

        txn.commit().unwrap();
    }

    /// Create new indices in the database defined by the provided commands.
    ///
    /// In case data is already stored, it will be propagated to the fresh
    /// created indices.
    ///
    /// In case the name used for the new index already exists, the existing
    /// index will be overwritten by the fresh one.
    pub fn create_indices(&mut self, commands: Vec<CreateFieldIndex>) {
        let mut txn = self.env.write_txn().unwrap();

        let mut indices_to_store: HashMap<&String, Index> = HashMap::new();

        let items = self
            .data
            .iter(&txn)
            .expect("Could not read data to create index")
            .map(|entry| entry.expect("Could not read entry while reading data to create index"));

        let mut descriptors = HashMap::new();

        // Iterate over each item and populate the data to the new indices
        for (id, item) in items {
            for command in &commands {
                let Some(value) = item.fields.get(&command.name).cloned() else {
                    continue;
                };

                let position = id_to_position(id);

                if let Some(index) = indices_to_store.get_mut(&command.name) {
                    index.put(value, position);
                } else {
                    // Create the new index and appended in memory, after it's populated
                    // with the item's data it will be stored.
                    let mut index = self
                        .read_index(&txn, &command.name)
                        .unwrap_or_else(|| Index::from_type(&command.descriptor));

                    index.put(value, position);
                    indices_to_store.insert(&command.name, index);
                    descriptors.insert(command.name.clone(), command.descriptor.clone());
                }
            }
        }

        // Update the stored indices with the new entries
        for (name, index) in indices_to_store {
            self.indices.put(&mut txn, name, &index).unwrap();
        }

        self.index_descriptors.extend(descriptors);

        txn.commit().unwrap();
    }

    /// Removes a number of items at once from the DB by their IDs.
    pub fn remove(&self, ids: &[DataItemId]) {
        let mut txn = self.env.write_txn().unwrap();
        let mut positions_to_delete = Vec::with_capacity(ids.len());

        for id in ids {
            // Remove item from data and ID to position mapping
            let present = self.data.delete(&mut txn, id).unwrap();
            if !present {
                continue;
            }

            // Categorize the item's position to be removed
            positions_to_delete.push(id_to_position(*id));
        }

        // Remove positions from the indices
        let mut entries = self
            .indices
            .iter_mut(&mut txn)
            .expect("Could not iterate indices from the DB.");

        while let Some(entry) = entries.next() {
            let (key, mut index) = entry
                .map(|(key, value)| (key.to_string(), value))
                .expect("Could not read entry while iterating indices from the DB.");

            for position in &positions_to_delete {
                index.remove_item(*position);
            }

            entries.put_current(&key, &index).unwrap();
        }

        drop(entries);

        // Remove positions from all the items that need to be deleted.
        if let Some(mut all) = self.get_all_positions(&txn) {
            for position in positions_to_delete {
                all.remove(position);
            }
            self.documents.put(&mut txn, ALL_ITEMS_KEY, &all).unwrap();
        }

        txn.commit().unwrap();
    }

    /// Get a `RoaringBitmap` for all the data's positions in the DB.
    ///
    /// Use the current transaction to don't create transactions implicitly, if not needed.
    fn get_all_positions(&self, txn: &RoTxn) -> Option<RoaringBitmap> {
        self.documents
            .get(txn, ALL_ITEMS_KEY)
            .expect("Could not read all items from the DB.")
    }

    /// Read an index from the DB by its field name.
    ///
    /// Use the current transaction to don't create transactions implicitly, if not needed.
    fn read_index(&self, txn: &RoTxn, field: &String) -> Option<Index> {
        self.indices
            .get(txn, field)
            .unwrap_or_else(|_| panic!("Could not read index with \"{}\" from the DB", field))
    }

    /// Read indices for a given set of fields. In case a field is not found, it won't be present
    /// in the returned `EntityIndices`.
    pub fn read_indices(&self, fields: &[String]) -> EntityIndices {
        let txn = self.env.read_txn().unwrap();

        let field_indices = fields
            .iter()
            .filter_map(|name| {
                self.read_index(&txn, name)
                    .map(|index| (name.to_string(), index))
            })
            .collect();

        let all = self
            .documents
            .get(&txn, ALL_ITEMS_KEY)
            .unwrap()
            .unwrap_or_default();

        EntityIndices { field_indices, all }
    }

    /// Read all the indices present in the storage.
    pub fn read_all_indices(&self) -> EntityIndices {
        let txn = self.env.read_txn().unwrap();

        let field_indices = self
            .indices
            .iter(&txn)
            .expect("Could not iterate indices from the DB.")
            .map(|item| {
                item.map(|(key, value)| (key.to_string(), value))
                    .expect("Could not read index from DB.")
            })
            .collect();

        let all = self
            .documents
            .get(&txn, ALL_ITEMS_KEY)
            .expect("Could not read ALL items index from DB.")
            .unwrap_or_default();

        EntityIndices { field_indices, all }
    }

    /// Read a data item from the storage using its identifier.
    pub(crate) fn read_by_id(&self, id: &DataItemId) -> Option<DataItem> {
        let txn = self.env.read_txn().unwrap();

        self.data
            .get(&txn, id)
            .expect("Could not read item from DB")
    }

    pub(crate) fn add_deltas(&self, date: Date, deltas: &[DeltaChange]) {
        let timestamp = I64::<BE>::new(date_to_timestamp(date));

        let mut txn = self.env.write_txn().unwrap();

        let mut current = self
            .deltas
            .get(&txn, &timestamp)
            .expect("Could not read deltas from DB")
            .unwrap_or_default();

        // Iterate over the deltas to create for each field name the before and after index
        for delta in deltas {
            let type_descriptor = self
                .index_descriptors
                .get(&delta.scope.field_name)
                .unwrap_or_else(|| {
                    panic!(
                        "Could not store delta. Field name \"{}\" is not available in the DB.",
                        &delta.scope.field_name
                    )
                });

            let stored_delta = current
                .entry(delta.scope.field_name.clone())
                .or_insert(StoredDelta::from_type(type_descriptor));

            let position = id_to_position(delta.scope.id);

            stored_delta.before.put(delta.before.clone(), position);
            stored_delta.after.put(delta.after.clone(), position);
        }

        self.deltas.put(&mut txn, &timestamp, &current).unwrap();

        txn.commit().unwrap();
    }
}

#[derive(Default, Debug)]
pub struct EntityIndices {
    /// Indices available associated by data's field name
    pub(crate) field_indices: HashMap<String, Index>,

    /// Bitmap including all items' positions
    pub(crate) all: RoaringBitmap,
}

pub struct CreateFieldIndex {
    pub name: String,
    pub descriptor: TypeDescriptor,
}
