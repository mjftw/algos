extern crate rand;

mod storage;
use crate::storage::kvstore::KVStore;
use rand::seq::SliceRandom;
use rand::Rng;
use std::iter::Zip;
use std::path::Path;

use storage::lsm_tree::LSMTree;

fn main() {
    let mut rng = rand::thread_rng();

    let mut lsm =
        LSMTree::<u32, String>::new(Path::new("/home/merlin/projects/algos/rust/algos/data"))
            .unwrap();

    let lorem: Vec<&str> = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Mauris risus erat, ultrices venenatis neque vitae, tincidunt lobortis augue. Morbi et dictum justo. Fusce rutrum libero non nibh dignissim eleifend. Donec viverra, diam eleifend laoreet consequat, augue dui luctus est, sit amet sodales nulla erat id neque. Aenean dignissim varius nunc id dapibus. Suspendisse sit amet nisl non felis volutpat placerat. Praesent posuere metus nec dolor scelerisque, ut vulputate sapien commodo."
        .split(" ").collect();

    for _ in 0..1000 {
        let k = rng.gen_range(1..10);
        let v = *lorem.choose(&mut rng).unwrap();

        lsm.put(k, v.to_string());
    }

    for k in 0..13 {
        println!("Searching for key {}", k);

        match lsm.get(&k) {
            Ok(Some(value)) => println!("Found value: {}", value),
            Ok(None) => println!("Value no found"),
            Err(err) => println!("Error: {}", err),
        };
    }
}
