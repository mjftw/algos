mod storage;
use std::path::Path;

use storage::lsm_tree::LSMTree;

use crate::storage::kvstore::KVStore;

fn main() {
    println!("Hello, world!");

    let mut lsm =
        LSMTree::<usize, String>::new(Path::new("/home/merlin/projects/algos/rust/algos/data"))
            .unwrap();

    let lorem = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Mauris risus erat, ultrices venenatis neque vitae, tincidunt lobortis augue. Morbi et dictum justo. Fusce rutrum libero non nibh dignissim eleifend. Donec viverra, diam eleifend laoreet consequat, augue dui luctus est, sit amet sodales nulla erat id neque. Aenean dignissim varius nunc id dapibus. Suspendisse sit amet nisl non felis volutpat placerat. Praesent posuere metus nec dolor scelerisque, ut vulputate sapien commodo.";

    lorem
        .split(" ")
        .enumerate()
        .for_each(|(k, v)| lsm.put(k, v.to_string()).unwrap());

    lorem
        .split(" ")
        .enumerate()
        .for_each(|(k, v)| lsm.put(k, v.to_string()).unwrap());

    lorem
        .split(" ")
        .enumerate()
        .for_each(|(k, v)| lsm.put(k, v.to_string()).unwrap());

    lorem
        .split(" ")
        .enumerate()
        .for_each(|(k, v)| lsm.put(k, v.to_string()).unwrap());
}
