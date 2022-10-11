use indexmap::IndexSet;
use std::hash::Hash;
use std::sync::Mutex;

pub trait PProfIdentifiable: Clone + Eq + Hash {
    fn set_id(&mut self, id: u64);
}

#[derive(Default)]
pub struct ProfileSet<T: PProfIdentifiable> {
    set: IndexSet<T>,
}

impl<T: PProfIdentifiable> ProfileSet<T> {
    pub fn add(&mut self, mut value: T) -> u64 {
        value.set_id((self.set.len() + 1).try_into().unwrap());
        let (index, _) = self.set.insert_full(value);
        (index + 1).try_into().unwrap()
    }

    pub fn export(&self) -> Vec<T> {
        self.set.iter().map(T::clone).collect()
    }
}

#[derive(Default)]
pub struct LockedProfileSet<T: PProfIdentifiable> {
    set: Mutex<ProfileSet<T>>,
}

impl<T: PProfIdentifiable> LockedProfileSet<T> {
    pub fn add(&self, value: T) -> u64 {
        let mut set = self.set.lock().unwrap();
        set.add(value)
    }

    pub fn export(&self) -> Vec<T> {
        let set = self.set.lock().unwrap();
        set.export()
    }
}
