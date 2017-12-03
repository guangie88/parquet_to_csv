extern crate pq_csv;

use pq_csv::parquet_to_csv;
use std::env;
use std::error::Error;
use std::fs::File;

fn run() -> Result<String, Box<Error>> {
    let args: Vec<_> = env::args().collect();

    if args.len() == 1 {
        Err("Please supply the data file path as argument")?;
    }

    let file_path = &args[1];
    let file = File::open(file_path)?;
    let v = vec![];

    Ok(String::from_utf8(parquet_to_csv(file, v)?)?)
}

fn main() {
    match run() {
        Ok(v) => println!("{:?}", v),
        Err(e) => println!("{:?}", e),
    }
}
