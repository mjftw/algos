pub trait KVStore<K, V, E> {
    fn put(&mut self, k: K, v: V) -> Result<(), E>;
    fn remove(&mut self, k: K) -> Result<(), E>;
    fn get(&self, k: &K) -> Result<Option<V>, E>;
}
