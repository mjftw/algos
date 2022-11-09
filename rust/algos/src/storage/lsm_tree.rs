extern crate ciborium;
extern crate serde;

use super::kvstore;
use super::rbtree::RBTree;
use serde::Serialize;
use std::collections::HashMap;
use std::hash::Hash;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::{fs, io};

type IOResult<T> = Result<T, io::Error>;

pub struct LSMTree<K: Ord + Hash, V> {
    storage_dir: PathBuf,
    segment_files: Vec<PathBuf>,
    index: HashMap<K, (u32, usize)>,
    records_per_index: usize,
    next_segment_num: u32,
    memtable: RBTree<K, V>,
    memtable_max_size: usize,
}

impl<K: Ord + Serialize + Hash + Copy, V: Serialize> LSMTree<K, V> {
    pub fn new(storage_dir: &Path) -> IOResult<Self> {
        fs::create_dir_all(storage_dir)?;

        Ok(LSMTree {
            storage_dir: storage_dir.to_owned(),
            segment_files: Vec::new(),
            index: HashMap::new(),
            records_per_index: 4,
            next_segment_num: 0,
            memtable: RBTree::new(),
            memtable_max_size: 16,
        })
    }

    fn memtable_over_size(&self) -> bool {
        //TODO: Use more intelligent and less inefficient size checking algorithm
        self.memtable.keys().count() > self.memtable_max_size
    }

    // fn compact_segments() TODO

    fn write_new_segment(&mut self) -> IOResult<&Path> {
        let segment_num = self.next_segment_num;
        let segment_path = &self.storage_dir.join(format!("{}", segment_num));

        let memtable_vec: Vec<(&K, &V)> = self.memtable.iter().collect();
        let mut buffer = Vec::<u8>::new();
        let mut index: HashMap<K, (u32, usize)> = HashMap::new();

        for kv_chunk in memtable_vec.chunks(self.records_per_index) {
            let key_offset = buffer.len();

            //TODO: Handle error
            ciborium::ser::into_writer(&self.memtable, &mut buffer);
            let (index_key, _) = *kv_chunk.first().unwrap();

            index.insert(*index_key, (segment_num, key_offset));
        }

        fs::File::create(segment_path)?.write_all(&buffer)?;

        self.segment_files.push(segment_path.to_path_buf());
        self.index.extend(index);

        self.memtable.clear();

        self.next_segment_num += 1;

        Ok(self.segment_files.last().unwrap())
    }
}

impl<K: Ord + Serialize + Hash + Copy, V: Serialize> kvstore::KVStore<K, V, io::Error>
    for LSMTree<K, V>
{
    fn put(&mut self, k: K, v: V) -> IOResult<()> {
        self.memtable.insert(k, v);

        if self.memtable_over_size() {
            self.write_new_segment()?;
        }

        Ok(())
    }

    fn get(&self, k: &K) -> IOResult<Option<&V>> {
        Ok(self.memtable.get(k))
    }
}
