use std::collections::HashMap;
use std::hash::Hash;
use std::sync::atomic::{AtomicU32, Ordering};

pub trait AtomicCounter<K, V> {
    fn new(keys: impl Iterator<Item = K>) -> Self;
    fn get(&self, key: &K) -> Option<V>;
    fn set(&self, key: &K, value: V);
    fn inc(&self, key: &K);
    fn dec(&self, key: &K);
    fn add(&self, key: &K, value: V);
    fn sub(&self, key: &K, value: V);
}

pub trait AtomicInteger<V> {
    fn one() -> V;
    fn load(&self, order: Ordering) -> V;
    fn store(&self, val: V, order: Ordering);
    fn fetch_add(&self, val: V, order: Ordering) -> V;
    fn fetch_sub(&self, val: V, order: Ordering) -> V;
    fn inc(&self, order: Ordering) -> V {
        self.fetch_add(Self::one(), order)
    }
    fn dec(&self, order: Ordering) -> V {
        self.fetch_sub(Self::one(), order)
    }
}

impl AtomicInteger<u32> for AtomicU32 {
    fn one() -> u32 {
        1
    }

    fn load(&self, order: Ordering) -> u32 {
        self.load(order)
    }

    fn store(&self, val: u32, order: Ordering) {
        self.store(val, order);
    }

    fn fetch_add(&self, val: u32, order: Ordering) -> u32 {
        self.fetch_add(val, order)
    }

    fn fetch_sub(&self, val: u32, order: Ordering) -> u32 {
        self.fetch_sub(val, order)
    }
}

impl<K, V, T> AtomicCounter<K, V> for HashMap<K, T>
where
    K: Eq + Hash + Clone,
    T: AtomicInteger<V> + Default,
{
    fn new(keys: impl Iterator<Item = K>) -> Self {
        keys.map(|key| (key, T::default())).collect()
    }

    fn get(&self, key: &K) -> Option<V> {
        self.get(key).map(|val| val.load(Ordering::Relaxed))
    }

    fn set(&self, key: &K, value: V) {
        self.get(key).map(|val| val.store(value, Ordering::Relaxed));
    }

    fn inc(&self, key: &K) {
        self.get(key).map(|val| val.inc(Ordering::Relaxed));
    }

    fn dec(&self, key: &K) {
        self.get(&key).map(|val| val.dec(Ordering::Relaxed));
    }

    fn add(&self, key: &K, value: V) {
        self.get(key)
            .map(|val| val.fetch_add(value, Ordering::Relaxed));
    }

    fn sub(&self, key: &K, value: V) {
        self.get(key)
            .map(|val| val.fetch_sub(value, Ordering::Relaxed));
    }
}
