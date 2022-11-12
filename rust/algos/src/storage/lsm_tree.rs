extern crate ciborium;
extern crate serde;

use super::kvstore;
use super::rbtree::RBTree;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Display;
use std::hash::Hash;
use std::io::{Seek, Write};
use std::path::{Path, PathBuf};
use std::{fs, io};

use algos::{GenericError, GenericResult};

pub struct LSMTree<K: Ord + Hash, V> {
    storage_dir: PathBuf,
    index: RBTree<K, (u32, u64)>,
    records_per_index: usize,
    next_segment_num: u32,
    memtable: RBTree<K, V>,
    memtable_max_size: usize,
}

impl<
        'de,
        K: Display + Ord + Copy + Serialize + Hash + Deserialize<'de>,
        V: Serialize + Deserialize<'de>,
    > LSMTree<K, V>
{
    pub fn new(
        storage_dir: &Path,
        memtable_max_size: usize,
        records_per_index: usize,
    ) -> GenericResult<Self> {
        fs::create_dir_all(storage_dir)?;

        Ok(LSMTree {
            storage_dir: storage_dir.to_owned(),
            index: RBTree::new(),
            records_per_index: records_per_index,
            next_segment_num: 0,
            memtable: RBTree::new(),
            memtable_max_size: memtable_max_size,
        })
    }

    fn memtable_over_size(&self) -> bool {
        //TODO: Use more intelligent and less inefficient size checking algorithm
        self.memtable.keys().count() > self.memtable_max_size
    }

    // fn compact_segments() TODO

    fn segment_path(&self, segment_num: u32) -> PathBuf {
        self.storage_dir.join(format!("{}", segment_num))
    }

    fn write_new_segment(&mut self) -> GenericResult<()> {
        let segment_num = self.next_segment_num;
        let memtable_vec: Vec<(&K, &V)> = self.memtable.iter().collect();
        let mut buffer = Vec::<u8>::new();
        let mut index: HashMap<K, (u32, u64)> = HashMap::new();

        for kv_chunk in memtable_vec.chunks(self.records_per_index) {
            let key_offset = buffer.len().try_into()?;

            ciborium::ser::into_writer(&self.memtable, &mut buffer)?;
            let (index_key, _) = *kv_chunk.first().unwrap();

            index.insert(*index_key, (segment_num, key_offset));
        }

        let segment_path = self.segment_path(segment_num);
        fs::File::create(segment_path)?.write_all(&buffer)?;

        self.index.extend(index);

        self.memtable.clear();

        self.next_segment_num += 1;

        Ok(())
    }

    fn get_from_memtable(&self, k: &K) -> Option<&V> {
        println!("Searching memtable for key: {}", k);
        self.memtable.get(k)
    }

    fn get_from_segments(&self, k: &K) -> GenericResult<Option<V>> {
        println!("Searching disc for key: {}", k);

        match self.index.iter().find(|(index_key, _)| **index_key < *k) {
            Some((_, (segment_num, offset))) => {
                let segment_path = self.segment_path(*segment_num);

                let mut segment = fs::File::open(segment_path)?;

                // TODO: Only read until next key rather than rest of file
                segment.seek(io::SeekFrom::Start(*offset))?;

                let buffer: Vec<(K, V)> = ciborium::de::from_reader(segment)?;

                let value = buffer
                    .into_iter()
                    .find(|(key, _)| *key == *k)
                    .and_then(|(_, value)| Some(value));

                Ok(value)
            }
            None => Ok(None),
        }
    }
}

//TODO: Would be much better if V did not have to be Copy!
// required for current get() implementation
impl<
        'de,
        K: Display + Ord + Copy + Serialize + Deserialize<'de> + Hash,
        V: Clone + Serialize + Deserialize<'de>,
    > kvstore::KVStore<K, V, GenericError> for LSMTree<K, V>
{
    fn put(&mut self, k: K, v: V) -> GenericResult<()> {
        self.memtable.insert(k, v);

        if self.memtable_over_size() {
            self.write_new_segment()?;
        }

        Ok(())
    }

    // Not nice to have to clone the value found in the memtable.
    // This is done because on memtable miss the value is read from disc and the variable then needs
    //  to be owned by something. This means that we can't return a reference to it!
    // If we can't return a reference to this then we can't return a refernce to the memtable hit
    //  value either.
    // This has the knock on effect of requiring that the value type implement Clone, which is not
    //  ideal.
    fn get(&self, k: &K) -> GenericResult<Option<V>> {
        Ok(self
            .get_from_memtable(k)
            .map(|found| found.to_owned())
            .or(self.get_from_segments(k)?))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rand::{seq::SliceRandom, Rng};

    use utils::run_test_with_temp_dir;

    use crate::storage::kvstore::KVStore;

    use super::LSMTree;

    #[test]
    fn write_readback_test_random() {
        let num_test_insertions = 10000;
        let num_test_unique_keys = 100;

        run_test_with_temp_dir(|temp_dir| {
            dbg!(temp_dir);

            let mut rng = rand::thread_rng();

            let mut lsm_tree = LSMTree::<u32, String>::new(
                temp_dir,
                num_test_insertions / 50,
                num_test_unique_keys / 10,
            )
            .unwrap();

            let lorem: Vec<&str> = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Mauris risus erat, ultrices venenatis neque vitae, tincidunt lobortis augue. Morbi et dictum justo. Fusce rutrum libero non nibh dignissim eleifend. Donec viverra, diam eleifend laoreet consequat, augue dui luctus est, sit amet sodales nulla erat id neque. Aenean dignissim varius nunc id dapibus. Suspendisse sit amet nisl non felis volutpat placerat. Praesent posuere metus nec dolor scelerisque, ut vulputate sapien commodo."
                .split(" ").collect();

            let store: HashMap<u32, &str> = (0..num_test_insertions)
                .into_iter()
                .map(|_| {
                    let k = rng.gen_range(1..num_test_unique_keys).try_into().unwrap();
                    let v = *lorem.choose(&mut rng).unwrap();

                    (k, v)
                })
                .collect();

            store.iter().for_each(|(k, v)| {
                lsm_tree.put(*k, v.to_string()).unwrap();
            });

            for (k, expected_v) in store {
                let result = lsm_tree.get(&k).unwrap();
                let expected = Some(expected_v.to_owned());

                assert_eq!(result, expected);
            }
        })
    }

    #[test]
    fn write_readback_test_sequential() {
        run_test_with_temp_dir(|temp_dir| {
            dbg!(temp_dir);

            let mut lsm_tree = LSMTree::new(temp_dir, 100, 10).unwrap();

            for i in 1..10000 {
                lsm_tree.put(i, format!("{}", i).to_string()).unwrap();
            }

            for i in 1..10000 {
                let result = lsm_tree.get(&i).unwrap();
                let expected = Some(format!("{}", i).to_string());

                assert_eq!(result, expected);
            }
        })
    }

    mod utils {
        extern crate tempdir;
        use std::{
            fs, io, panic,
            path::{Path, PathBuf},
        };

        use tempdir::TempDir;

        pub fn run_test_with_temp_dir<T>(test: T) -> ()
        where
            T: FnOnce(&Path) -> () + panic::UnwindSafe,
        {
            let temp_dir = create_temp_dir("rs_algos_test").unwrap();

            let result = panic::catch_unwind(|| test(&temp_dir));

            remove_temp_dir(&temp_dir).unwrap();

            assert!(result.is_ok())
        }

        fn create_temp_dir(prefix: &str) -> Result<PathBuf, io::Error> {
            let tmp_dir = TempDir::new(prefix)?;
            Ok(tmp_dir.into_path())
        }

        fn remove_temp_dir(path: &Path) -> Result<(), io::Error> {
            if path.is_absolute() && path.starts_with(Path::new("/tmp")) {
                fs::remove_dir_all(path)
            } else {
                panic!(
                    "Refusing to remove directory that does not start with /tmp: {}",
                    path.to_string_lossy()
                );
            }
        }
    }
}
