extern crate rbtree;
use super::kvstore;
use super::rbtree::RBTree;
use serde::Serialize;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::{fs, io};

type IOResult<T> = Result<T, io::Error>;

pub struct LSMTree<K: Ord, V> {
    storage_dir: PathBuf,
    segment_files: Vec<PathBuf>,
    next_segment_index: u32,
    memtable: RBTree<K, V>,
    memtable_max_size: usize,
}

impl<K: Ord + Serialize, V: Serialize> LSMTree<K, V> {
    pub fn new(storage_dir: &Path) -> IOResult<Self> {
        fs::create_dir_all(storage_dir)?;

        Ok(LSMTree {
            storage_dir: storage_dir.to_owned(),
            segment_files: Vec::new(),
            next_segment_index: 0,
            memtable: RBTree::new(),
            memtable_max_size: 16,
        })
    }

    fn memtable_over_size(&self) -> bool {
        //TODO: Use more intelligent and less inefficient size checking algorithm
        self.memtable.keys().count() > self.memtable_max_size
    }

    // fn compact_segments() TODO

    fn serialise_memtable(&self) -> Result<Vec<u8>, serde_json::Error> {
        // TODO: Replace with efficient binary serialisation, or use a passed in serialiser
        serde_json::to_vec(&self.memtable)
    }

    fn write_new_segment(&mut self) -> IOResult<&Path> {
        let segment_path = &self
            .storage_dir
            .join(format!("seg_{}", self.next_segment_index));

        fs::File::create(segment_path)?.write_all(&self.serialise_memtable()?)?;

        self.memtable.clear();

        self.segment_files.push(segment_path.to_path_buf());
        self.next_segment_index += 1;

        Ok(self.segment_files.last().unwrap())
    }
}

impl<K: Ord + Serialize, V: Serialize> kvstore::KVStore<K, V, io::Error> for LSMTree<K, V> {
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
