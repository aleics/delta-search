use std::borrow::Cow;
use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::path::Path;

use heed::byteorder::BigEndian;
use heed::{types::*, BoxedError, BytesDecode, BytesEncode};
use heed::{Database, Env, EnvOpenOptions, RoTxn};
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::data::{date_to_timestamp, DataItem};
use crate::index::{Index, IndexError, TypeDescriptor};
use crate::query::{DeltaChange, DeltaScope};
use crate::DataItemId;

pub(crate) const DB_FOLDER: &str = "./delta-db";
const DATA_DB_NAME: &str = "data";
const INDICES_DB_NAME: &str = "indices";
const DOCUMENTS_DB_NAME: &str = "documents";
const DELTAS_DB_NAME: &str = "deltas";

const ALL_ITEMS_KEY: &str = "__all";

pub(crate) fn position_to_id(position: u32) -> DataItemId {
    u64::from(position)
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

    pub fn build(&self) -> Result<EntityStorage, StorageError> {
        let name = self.name.as_ref().ok_or_else(|| StorageError::NoName)?;

        EntityStorage::init(name)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredDelta {
    affected: RoaringBitmap,
    field_name: String,
    before: Index,
    after: Index,
}

impl StoredDelta {
    fn from_type(field_name: String, descriptor: &TypeDescriptor) -> Self {
        StoredDelta {
            affected: RoaringBitmap::new(),
            field_name,
            before: Index::from_type(descriptor),
            after: Index::from_type(descriptor),
        }
    }
}

type BEU64 = U64<BigEndian>;

#[derive(Debug, PartialEq)]
struct DeltaKey {
    context: u64,
    timestamp: i64,
}

impl DeltaKey {
    fn new(context: Option<u32>, timestamp: i64) -> Self {
        let context = context.map(|context| context as u64 + 1).unwrap_or(0);
        DeltaKey { context, timestamp }
    }
}

struct DeltaKeyCodec;

impl<'a> BytesEncode<'a> for DeltaKeyCodec {
    type EItem = DeltaKey;

    fn bytes_encode(key: &'a Self::EItem) -> Result<Cow<'a, [u8]>, BoxedError> {
        let context_bytes = key.context.to_be_bytes();
        let timestamp_bytes = key.timestamp.to_be_bytes();

        let mut output = Vec::new();
        output.extend_from_slice(&context_bytes);
        output.extend_from_slice(&timestamp_bytes);
        Ok(Cow::Owned(output))
    }
}

impl<'a> BytesDecode<'a> for DeltaKeyCodec {
    type DItem = DeltaKey;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
        let context_start = 0;
        let context_end = size_of::<u64>();
        let timestamp_start = context_end;
        let timestamp_end = context_end + size_of::<i64>();

        let bytes = bytes.to_vec();

        let context = match bytes.get(context_start..context_end) {
            Some(bytes) => bytes.try_into().map(u64::from_be_bytes).unwrap(),
            None => return Err("invalid log key: cannot extract context".into()),
        };

        let timestamp = match bytes.get(timestamp_start..timestamp_end) {
            Some(bytes) => bytes.try_into().map(i64::from_be_bytes).unwrap(),
            None => return Err("invalid log key: cannot extract timestamp".into()),
        };

        Ok(DeltaKey { context, timestamp })
    }
}

struct DeltaKeyContextCodec;

impl<'a> BytesEncode<'a> for DeltaKeyContextCodec {
    type EItem = u64;

    fn bytes_encode(context: &'a Self::EItem) -> Result<Cow<'a, [u8]>, BoxedError> {
        let context_bytes = context.to_be_bytes();
        Ok(Cow::Owned(context_bytes.to_vec()))
    }
}

impl<'a> BytesDecode<'a> for DeltaKeyContextCodec {
    type DItem = DeltaKey;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
        DeltaKeyCodec::bytes_decode(bytes)
    }
}

/// Storage in disk using `LMDB` for the data and their related indices.
pub struct EntityStorage {
    pub(crate) id: String,
    env: Env,

    /// Database storing the entity's data items, where the key is the data
    /// item id and the value the data item itself.
    data: Database<BEU64, SerdeBincode<DataItem>>,

    /// Database storing the entity's indices, where the key is the field name
    /// and the value is the index.
    indices: Database<Str, SerdeBincode<Index>>,

    /// Database storing custom bitmaps needed to index the data items'
    /// and their positions in the indices.
    documents: Database<Str, SerdeBincode<RoaringBitmap>>,

    /// Database to store deltas for each scope and affected field. The key
    /// is an identifier for each delta scope, and the value is a map
    /// of deltas for each field.
    deltas: Database<DeltaKeyCodec, SerdeBincode<HashMap<String, StoredDelta>>>,

    /// An in-memory key-value map to store type descriptors for each index.
    /// This is propagated during initialization.
    index_descriptors: papaya::HashMap<String, TypeDescriptor>,
}

impl EntityStorage {
    /// Initialises a new `DiskStorage` instance by creating the necessary files
    /// and LMDB `Database` entries.
    pub fn init(name: &str) -> Result<Self, StorageError> {
        let file_name = format!("{}.mdb", name);
        let path = Path::new(DB_FOLDER).join(file_name);

        std::fs::create_dir_all(&path)?;

        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(100 * 1024 * 1024) // 100 MB max size
                .max_dbs(3000)
                .open(path)?
        };

        let mut txn = env.write_txn()?;

        let data = env
            .create_database(&mut txn, Some(DATA_DB_NAME))
            .map_err(|_| StorageError::CreateDatabase(DATA_DB_NAME))?;

        let indices = env
            .create_database(&mut txn, Some(INDICES_DB_NAME))
            .map_err(|_| StorageError::CreateDatabase(INDICES_DB_NAME))?;

        let documents = env
            .create_database(&mut txn, Some(DOCUMENTS_DB_NAME))
            .map_err(|_| StorageError::CreateDatabase(DOCUMENTS_DB_NAME))?;

        let deltas = env
            .create_database(&mut txn, Some(DELTAS_DB_NAME))
            .map_err(|_| StorageError::CreateDatabase(DELTAS_DB_NAME))?;

        txn.commit()?;

        let mut storage = EntityStorage {
            id: name.to_string(),
            env,
            indices,
            documents,
            data,
            deltas,
            index_descriptors: Default::default(),
        };

        storage.propagate_indices()?;

        Ok(storage)
    }

    /// Propagate the index data into other in-memory data used for faster access to certain
    /// properties and reduce deserialization overhead while running certain operations.
    fn propagate_indices(&mut self) -> Result<(), StorageError> {
        let txn = self.env.read_txn().unwrap();

        let entries = self.indices.iter(&txn)?;

        for entry in entries {
            let (name, index) = entry?;
            self.index_descriptors
                .pin()
                .insert(name.to_string(), index.create_descriptor());
        }

        Ok(())
    }

    /// Get the current entity's storage path.
    pub(crate) fn get_path(&self) -> &Path {
        self.env.path()
    }

    /// Fill the DB with data by clearing the previous one. This is meant for when initialising
    /// the storage and remove any previous data.
    pub fn carry(&self, data: &[DataItem]) -> Result<(), StorageError> {
        self.clear()?;
        self.add_multiple(data)?;

        Ok(())
    }

    /// Clears the current storage indices and data.
    pub fn clear(&self) -> Result<(), StorageError> {
        let mut txn = self.env.write_txn()?;

        self.data.clear(&mut txn)?;
        self.indices.clear(&mut txn)?;
        self.documents.clear(&mut txn)?;
        self.index_descriptors.pin().clear();

        txn.commit()?;

        Ok(())
    }

    /// Add multiple items using chunks so that multiple transactions are commited, depending on
    /// the amount of chunks generated.
    fn add_multiple(&self, data: &[DataItem]) -> Result<(), StorageError> {
        // Add elements in chunks to optimise the storing execution write operations in bulk.
        for chunk in data.chunks(100) {
            self.add(chunk)?;
        }

        Ok(())
    }

    /// Store an amount of items in the database using a single transaction.
    /// Any index is as well updated with the stored items after the transaction
    /// is committed.
    pub fn add(&self, items: &[DataItem]) -> Result<(), StorageError> {
        let mut txn = self.env.write_txn()?;

        let mut indices_to_store: HashMap<String, Index> = HashMap::new();
        let mut all = self.documents.get(&txn, ALL_ITEMS_KEY)?.unwrap_or_default();

        for item in items {
            // Read item ID and determine position
            let position = id_to_position(item.id);

            // Insert item in the data DB
            self.data.put(&mut txn, &item.id, item)?;

            // Update indices in memory with the item data to reduce (de)serialization overhead
            // if we update index one by one in the DB.
            for (index_name, index_descriptor) in self.index_descriptors.pin().iter() {
                let Some(value) = item.fields.get(index_name).cloned() else {
                    continue;
                };

                if let Some(index) = indices_to_store.get_mut(index_name) {
                    index.put(value, position)?;
                } else {
                    let mut index = self
                        .indices
                        .get(&txn, index_name)?
                        .unwrap_or_else(|| Index::from_type(index_descriptor));

                    index.put(value, position)?;
                    indices_to_store.insert(index_name.clone(), index);
                }
            }

            all.insert(position);
        }

        // Store indices in the DB for each index that has been changed.
        self.documents.put(&mut txn, ALL_ITEMS_KEY, &all)?;

        for (name, index) in indices_to_store {
            self.indices.put(&mut txn, &name, &index)?;
        }

        txn.commit()?;

        Ok(())
    }

    /// Create new indices in the database defined by the provided commands.
    ///
    /// In case data is already stored, it will be propagated to the fresh
    /// created indices.
    ///
    /// In case the name used for the new index already exists, the existing
    /// index will be overwritten by the fresh one.
    pub fn create_indices(&self, commands: Vec<CreateFieldIndex>) -> Result<(), StorageError> {
        let mut txn = self.env.write_txn()?;

        let mut indices_to_store: HashMap<&String, Index> = HashMap::new();

        let entries = self.data.iter(&txn)?;

        // Iterate over each item and populate the data to the new indices
        for entry in entries {
            let (id, item) = entry?;
            for command in &commands {
                let Some(value) = item.fields.get(&command.name).cloned() else {
                    continue;
                };

                let position = id_to_position(id);

                if let Some(index) = indices_to_store.get_mut(&command.name) {
                    index.put(value, position)?;
                } else {
                    // Create the new index and appended in memory, after it's populated
                    // with the item's data it will be stored.
                    let mut index = self
                        .indices
                        .get(&txn, &command.name)?
                        .unwrap_or_else(|| Index::from_type(&command.descriptor));

                    index.put(value, position)?;
                    indices_to_store.insert(&command.name, index);
                    self.index_descriptors
                        .pin()
                        .insert(command.name.clone(), command.descriptor.clone());
                }
            }
        }

        // Update the stored indices with the new entries
        for (name, index) in indices_to_store {
            self.indices.put(&mut txn, name, &index)?;
        }

        txn.commit()?;

        Ok(())
    }

    /// Removes a number of items at once from the DB by their IDs.
    pub fn remove(&self, ids: &[DataItemId]) -> Result<(), StorageError> {
        let mut txn = self.env.write_txn()?;
        let mut positions_to_delete = Vec::with_capacity(ids.len());

        for id in ids {
            // Remove item from data and ID to position mapping
            let present = self.data.delete(&mut txn, id)?;
            if !present {
                continue;
            }

            // Categorize the item's position to be removed
            positions_to_delete.push(id_to_position(*id));
        }

        // Remove positions from the indices
        let mut entries = self.indices.iter_mut(&mut txn)?;

        while let Some(entry) = entries.next() {
            let (key, mut index) = entry.map(|(key, value)| (key.to_string(), value))?;

            for position in &positions_to_delete {
                index.remove_item(*position);
            }

            unsafe { entries.put_current(&key, &index)? };
        }

        drop(entries);

        // Remove positions from all the items that need to be deleted.
        if let Some(mut all) = self.documents.get(&txn, ALL_ITEMS_KEY)? {
            for position in positions_to_delete {
                all.remove(position);
            }
            self.documents.put(&mut txn, ALL_ITEMS_KEY, &all)?;
        }

        txn.commit()?;

        Ok(())
    }

    /// Read the entity indices from the DB by their field names.
    ///
    /// Use the current transaction to don't create transactions implicitly, if not needed.
    fn read_indices(&self, txn: &RoTxn, fields: &[String]) -> Result<EntityIndices, StorageError> {
        let mut field_indices = HashMap::with_capacity(fields.len());

        for field in fields {
            if let Some(index) = self.indices.get(txn, field)? {
                field_indices.insert(field.to_string(), index);
            }
        }

        let all = self.documents.get(txn, ALL_ITEMS_KEY)?.unwrap_or_default();

        Ok(EntityIndices {
            field_indices,
            all,
            affected: AffectedData::default(),
        })
    }

    /// Read all the entity indices from the DB.
    ///
    /// Use the current transaction to don't create transactions implicitly, if not needed.
    fn read_all_indices(&self, txn: &RoTxn) -> Result<EntityIndices, StorageError> {
        let mut field_indices = HashMap::new();

        for item in self.indices.iter(txn)? {
            let (field, index) = item?;
            field_indices.insert(field.to_string(), index);
        }

        let all = self.documents.get(txn, ALL_ITEMS_KEY)?.unwrap_or_default();

        Ok(EntityIndices {
            field_indices,
            all,
            affected: AffectedData::default(),
        })
    }

    fn read_deltas(
        &self,
        txn: &RoTxn,
        scope: &DeltaScope,
    ) -> Result<HashMap<String, StoredDelta>, StorageError> {
        let scope_timestamp = date_to_timestamp(scope.date);
        let scope_key = DeltaKey::new(scope.context, scope_timestamp);

        let deltas_by_date = self
            .deltas
            // Use the `DeltaKeyContextCodec` to read deltas using only the `context` as a prefix
            // and iterate over keys ascending (lower timestamp to higher).
            .remap_key_type::<DeltaKeyContextCodec>()
            .rev_prefix_iter(txn, &scope_key.context)?;

        let mut aggregated_deltas: HashMap<String, StoredDelta> = HashMap::new();
        for entry in deltas_by_date {
            let (stored_delta_key, stored_deltas) = entry?;

            // A stored delta is part of the scope, if it occurs the same date or before
            // the scope's date (stored timestamp is smaller or equal than the scope's timestamp)
            if stored_delta_key.timestamp <= scope_timestamp {
                for (field, stored_delta) in stored_deltas {
                    if let Some(aggregated_delta) = aggregated_deltas.get_mut(&field) {
                        aggregated_delta.before.plus(&stored_delta.before)?;
                        aggregated_delta.after.plus(&stored_delta.after)?;
                        aggregated_delta.affected |= &stored_delta.affected;
                    } else {
                        aggregated_deltas.insert(field, stored_delta);
                    }
                }
            } else {
                // Since we are iterating in a sorted manner (lower to higher), once we've reached
                // a delta with a higher timestamp than the scope's timestamp, the rest is also
                // not part of the scope.
                break;
            }
        }

        Ok(aggregated_deltas)
    }

    fn apply_deltas(
        deltas: HashMap<String, StoredDelta>,
        existing: &mut EntityIndices,
    ) -> Result<AffectedData, StorageError> {
        let mut affected = AffectedData::default();

        for (field_name, stored_delta) in &deltas {
            if let Some(index) = existing.field_indices.get_mut(field_name) {
                index.minus(&stored_delta.before)?;
                index.plus(&stored_delta.after)?;

                affected.items |= &stored_delta.affected;
                affected.fields.push(field_name.clone());
            }
        }

        Ok(affected)
    }

    pub fn read_indices_in(
        &self,
        scope: &DeltaScope,
        fields: &[String],
    ) -> Result<EntityIndices, StorageError> {
        let txn = self.env.read_txn().unwrap();

        let deltas = self.read_deltas(&txn, scope)?;

        let mut indices = if deltas.is_empty() {
            self.read_indices(&txn, fields)?
        } else {
            let mut fields = fields.to_vec();
            fields.extend(deltas.keys().cloned());
            self.read_indices(&txn, &fields)?
        };

        let affected = EntityStorage::apply_deltas(deltas, &mut indices)?;

        Ok(indices.with_affected(affected))
    }

    pub fn read_all_indices_in(&self, scope: &DeltaScope) -> Result<EntityIndices, StorageError> {
        let txn = self.env.read_txn().unwrap();

        let deltas = self.read_deltas(&txn, scope)?;
        let mut indices = self.read_all_indices(&txn)?;

        let affected = EntityStorage::apply_deltas(deltas, &mut indices)?;

        Ok(indices.with_affected(affected))
    }

    /// Read indices for a given set of fields. In case a field is not found, it won't be present
    /// in the returned `EntityIndices`.
    pub fn read_current_indices(&self, fields: &[String]) -> Result<EntityIndices, StorageError> {
        let txn = self.env.read_txn().unwrap();
        self.read_indices(&txn, fields)
    }

    /// Read all the indices present in the storage.
    pub fn read_all_current_indices(&self) -> Result<EntityIndices, StorageError> {
        let txn = self.env.read_txn().unwrap();
        self.read_all_indices(&txn)
    }

    /// Read multiple data items given an iterator of item IDs. The provided
    /// index is used to overwrite any affected data by a delta change.
    pub(crate) fn read_multiple<'a, T>(
        &self,
        ids: T,
        indices: &EntityIndices,
    ) -> Result<Vec<DataItem>, StorageError>
    where
        T: Iterator<Item = &'a DataItemId>,
    {
        let txn = self.env.read_txn().unwrap();

        let mut data = Vec::new();

        for id in ids {
            let Some(mut item) = self.data.get(&txn, id)? else {
                continue;
            };

            let position = id_to_position(item.id);
            if indices.affected.items.contains(position) {
                for field_name in &indices.affected.fields {
                    if let Some(value) = indices
                        .field_indices
                        .get(field_name)
                        .and_then(|index| index.get_value(position))
                    {
                        item.fields.insert(field_name.clone(), value);
                    }
                }
            }

            data.push(item);
        }

        Ok(data)
    }

    /// Store deltas in the database by a given `scope`.
    pub(crate) fn add_deltas(
        &self,
        scope: &DeltaScope,
        deltas: Vec<DeltaChange>,
    ) -> Result<(), StorageError> {
        let scope_timestamp = date_to_timestamp(scope.date);
        let scope_key = DeltaKey::new(scope.context, scope_timestamp);

        let mut txn = self.env.write_txn()?;

        let mut current = self.deltas.get(&txn, &scope_key)?.unwrap_or_default();

        // Group deltas by field, so we reduce amount of calls to the database
        let deltas_by_field: HashMap<String, Vec<DeltaChange>> =
            deltas.into_iter().fold(HashMap::new(), |mut acc, delta| {
                let entry = acc.entry(delta.field_name.clone()).or_default();
                entry.push(delta);
                acc
            });

        // Iterate over the deltas of each field and store a delta with the `before` value.
        // This value is used during query time when applying delta changes to the indices.
        for (field_name, deltas) in deltas_by_field.iter() {
            let stored_delta = current.entry(field_name.clone()).or_insert_with(|| {
                let binding = self.index_descriptors.pin();
                let type_descriptor = binding.get(field_name).unwrap_or_else(|| {
                    panic!(
                        "Could not store delta. Field name \"{}\" is not available in the DB.",
                        field_name
                    )
                });

                StoredDelta::from_type(field_name.clone(), type_descriptor)
            });

            // Read the current index and attach to the stored delta
            let index = self.indices.get(&txn, field_name)?;

            for delta in deltas {
                let position = id_to_position(delta.id);

                if let Some(before) = index.as_ref().and_then(|index| index.get_value(position)) {
                    stored_delta.before.put(before, position)?;
                }

                stored_delta.after.put(delta.after.clone(), position)?;
                stored_delta.affected.insert(position);
            }
        }

        self.deltas.put(&mut txn, &scope_key, &current)?;

        txn.commit()?;

        Ok(())
    }
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum StorageError {
    #[error("no name has been defined for an entity storage instance")]
    NoName,
    #[error("there was an error creating database for key `{0}`")]
    CreateDatabase(&'static str),
    #[error(transparent)]
    CreateStoragePath(#[from] std::io::Error),
    #[error(transparent)]
    DbOperation(#[from] heed::Error),
    #[error(transparent)]
    Index(#[from] IndexError),
}

#[derive(Default, Debug)]
pub struct EntityIndices {
    /// Indices available associated by data's field name
    pub(crate) field_indices: HashMap<String, Index>,

    /// Bitmap including all items' positions
    pub(crate) all: RoaringBitmap,

    /// Bitmap including items' positions that are affected by
    pub(crate) affected: AffectedData,
}

impl EntityIndices {
    fn with_affected(mut self, affected: AffectedData) -> Self {
        self.affected = affected;
        self
    }
}

#[derive(Debug, Default)]
pub(crate) struct AffectedData {
    pub(crate) items: RoaringBitmap,
    pub(crate) fields: Vec<String>,
}

pub struct CreateFieldIndex {
    pub name: String,
    pub descriptor: TypeDescriptor,
}

#[cfg(test)]
mod tests {
    use heed::{BytesDecode, BytesEncode};
    use lazy_static::lazy_static;
    use time::{Date, Month};

    use super::{date_to_timestamp, DeltaKey, DeltaKeyCodec};

    lazy_static! {
        static ref DATE: Date = Date::from_calendar_date(2023, Month::January, 1).unwrap();
    }

    #[test]
    fn encodes_decodes_delta_keys() {
        // given
        let key = DeltaKey::new(Some(0), date_to_timestamp(*DATE));

        // when
        let encoded = DeltaKeyCodec::bytes_encode(&key).unwrap();
        let decoded = DeltaKeyCodec::bytes_decode(&encoded).unwrap();

        // then
        assert_eq!(key, decoded);
    }
}
