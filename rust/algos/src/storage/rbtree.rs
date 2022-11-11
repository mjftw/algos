extern crate rbtree;
use serde::{Serialize, Serializer};
use std::ops::{Deref, DerefMut};

use algos::GenericResult;

pub struct RBTree<K: Ord, V>(rbtree::RBTree<K, V>);

impl<K: Ord, V> RBTree<K, V> {
    pub fn new() -> Self {
        RBTree(rbtree::RBTree::new())
    }
}

impl<K: Ord, V> Deref for RBTree<K, V> {
    type Target = rbtree::RBTree<K, V>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<K: Ord, V> DerefMut for RBTree<K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<K: Ord + Serialize, V: Serialize> Serialize for RBTree<K, V> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_seq(self.iter())
    }
}

impl<K: Ord, V> From<rbtree::RBTree<K, V>> for RBTree<K, V> {
    fn from(t: rbtree::RBTree<K, V>) -> Self {
        RBTree(t)
    }
}
