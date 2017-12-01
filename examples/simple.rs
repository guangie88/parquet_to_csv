extern crate pq_csv;

use pq_csv::parquet_to_csv;
use std::error::Error;
use std::fs::File;

fn run() -> Result<String, Box<Error>> {
    const DATA_FILE: &str = "data/test-simple.parq";

    let file = File::open(DATA_FILE)?;
    let v = vec![];

    Ok(String::from_utf8(parquet_to_csv(file, v)?)?)
}

fn main() {
    match run() {
        Ok(v) => println!("{:?}", v),
        Err(e) => println!("{:?}", e),
    }
}
