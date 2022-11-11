extern crate ciborium;
extern crate serde;

use super::kvstore;
use super::rbtree::RBTree;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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
    deserialize_buffer: Vec<(K, V)>,
}

impl<'de, K: Ord + Copy + Serialize + Hash + Deserialize<'de>, V: Serialize + Deserialize<'de>>
    LSMTree<K, V>
{
    pub fn new(storage_dir: &Path) -> GenericResult<Self> {
        fs::create_dir_all(storage_dir)?;

        Ok(LSMTree {
            storage_dir: storage_dir.to_owned(),
            index: RBTree::new(),
            records_per_index: 10,
            next_segment_num: 0,
            memtable: RBTree::new(),
            memtable_max_size: 50,
            deserialize_buffer: Vec::new(),
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

    fn get_from_segments(&self, k: &K) -> GenericResult<Option<V>> {
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
        K: Ord + Copy + Serialize + Deserialize<'de> + Hash,
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

    fn get(&self, k: &K) -> GenericResult<Option<V>> {
        Ok(self
            .memtable
            .get(k)
            .map(|found| found.to_owned())
            .or(self.get_from_segments(k)?))
    }
}
