extern crate rbtree;
use serde::{Serialize, Serializer};
use std::{
    fmt::{Debug, Display},
    ops::{Deref, DerefMut},
};

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

impl<K: Display + Ord, V: Display> Display for RBTree<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            &self
                .iter()
                .map(|(k, v)| format!("({}, {})", k, v))
                .collect::<Vec<String>>()
                .join(", "),
        )
    }
}

impl<K: Debug + Ord, V: Debug> Debug for RBTree<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            &self
                .iter()
                .map(|(k, v)| format!("({:?}, {:?})", k, v))
                .collect::<Vec<String>>()
                .join(", "),
        )
    }
}
