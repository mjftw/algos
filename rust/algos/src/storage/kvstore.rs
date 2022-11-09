pub trait KVStore<K, V, E> {
    fn put(&mut self, k: K, v: V) -> Result<(), E>;
    fn get(&self, k: &K) -> Result<Option<&V>, E>;
}

// impl<K: Eq + Hash, V> KVStore<K, V, String> for HashMap<K, V> {
//     fn get(&self, k: &K) -> Result<Option<&V>, String> {
//         Ok(HashMap::get(&self, k))
//     }

//     fn put(&self, k: &K, v: V) -> Result<(), String> {
//         HashMap::put(&self, k, v)
//     }
// }
