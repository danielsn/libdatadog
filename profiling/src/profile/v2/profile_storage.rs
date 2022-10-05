use super::{Function, IndexSet, Location, Mapping};
use std::hash::Hash;
use std::sync::{Mutex, MutexGuard};

#[derive(Default)]
pub struct ProfileStorage {
    functions: IndexSet<Function>,
    locations: IndexSet<Location>,
    mappings: IndexSet<Mapping>,
    // string_table is intentionally missing; it needs finer lock granularity.
}

impl ProfileStorage {
    pub fn new() -> Self {
        Self::default()
    }

    fn index_to_id(index: usize) -> u64 {
        (index + 1).try_into().unwrap()
    }

    fn add<T: Hash + Eq>(set: &mut IndexSet<T>, value: T) -> u64 {
        let (index, _) = set.insert_full(value);
        Self::index_to_id(index)
    }

    pub fn add_function(&mut self, mut function: Function) -> u64 {
        // Adjust id before inserting it in case it's a new one.
        function.id = Self::index_to_id(self.functions.len());
        Self::add(&mut self.functions, function)
    }

    pub fn add_location(&mut self, mut location: Location) -> u64 {
        // Adjust id before inserting it in case it's a new one.
        location.id = Self::index_to_id(self.locations.len());
        Self::add(&mut self.locations, location)
    }

    pub fn add_mapping(&mut self, mut mapping: Mapping) -> u64 {
        // Adjust id before inserting it in case it's a new one.
        mapping.id = Self::index_to_id(self.mappings.len());
        Self::add(&mut self.mappings, mapping)
    }

    pub fn functions(&self) -> Vec<Function> {
        self.functions.iter().map(Function::clone).collect()
    }

    pub fn locations(&self) -> Vec<Location> {
        self.locations.iter().map(Location::clone).collect()
    }

    pub fn mappings(&self) -> Vec<Mapping> {
        self.mappings.iter().map(Mapping::clone).collect()
    }
}

#[derive(Default)]
pub struct LockedProfileStorage {
    storage: Mutex<ProfileStorage>,
}

impl LockedProfileStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn lock(&self) -> MutexGuard<ProfileStorage> {
        self.storage.lock().unwrap()
    }
}

impl From<ProfileStorage> for LockedProfileStorage {
    fn from(storage: ProfileStorage) -> Self {
        let storage = Mutex::new(storage);
        Self { storage }
    }
}
