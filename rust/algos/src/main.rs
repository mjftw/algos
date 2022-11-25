use std::fs;

extern crate rand;

mod storage;

fn main() {
    println!("Hello algos!");

    read();
}

fn read<'de>() {
    let file = fs::File::open("/tmp/rs_algos_test.tJff99ZxcvY5/0").unwrap();
    let result =
        ciborium::de::from_reader::<'de, Vec<(u32, storage::lsm_tree::Record<String>)>, _>(file)
            .unwrap();

    dbg!(result);
}
