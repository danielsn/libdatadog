use indexmap::IndexSet;
use std::sync::{Mutex, MutexGuard};

pub struct StringTable {
    set: IndexSet<String>,
}

impl StringTable {
    pub fn new() -> Self {
        Self {
            set: IndexSet::from([String::new()]),
        }
    }

    pub fn intern<S: Into<String> + AsRef<str>>(&mut self, string: S) -> i64 {
        let index = if let Some((index, _)) = self.set.get_full(string.as_ref()) {
            index
        } else {
            self.set.insert_full(string.into()).0
        };
        index.try_into().unwrap()
    }

    pub fn strings(&self) -> Vec<String> {
        self.set.iter().map(String::clone).collect()
    }
}

impl Default for StringTable {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Default)]
pub struct LockedStringTable {
    string_table: Mutex<StringTable>,
}

impl LockedStringTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Often we need to insert more than one string at a time, so rather than
    /// expose an intern method directly, we provide a lock() routine to get
    /// the lock yourself. Of course, be careful not to deadlock!
    pub fn lock(&self) -> MutexGuard<StringTable> {
        self.string_table.lock().unwrap()
    }

    pub fn strings(&self) -> Vec<String> {
        let string_table = self.lock();
        string_table.strings()
    }
}

impl From<StringTable> for LockedStringTable {
    fn from(string_table: StringTable) -> Self {
        let string_table = Mutex::new(string_table);
        Self { string_table }
    }
}
