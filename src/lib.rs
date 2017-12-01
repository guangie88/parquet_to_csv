extern crate csv;
extern crate parquet;

use self::csv::Writer;
use self::parquet::column::*;
use self::parquet::errors::ParquetError;
use self::parquet::file::reader::{FileReader, SerializedFileReader};
use std::error::Error;
use std::fs::File;
use std::io::Write;

pub fn parquet_to_csv<W>(file: File, w: W) -> Result<W, Box<Error>>
where W: Write + 'static 
{
    let reader = SerializedFileReader::new(file)?;
    let row_count = reader.num_row_groups();

    let row_groups = (0..row_count)
        .map(|r| reader.get_row_group(r))
        .collect::<Result<Vec<_>, ParquetError>>()?;

    let mut wtr = Writer::from_writer(w);

    for row_reader in row_groups.into_iter() {
        let col_count = row_reader.num_columns();

        let col_values = (0..col_count)
            .map(|c| row_reader.get_column_reader(c))
            .collect::<Result<Vec<_>, ParquetError>>()?
            .into_iter()
            .map(|col_reader| {
                match col_reader {
                    _ => "x"
                }
            });

        wtr.write_record(col_values)?
    }

    Ok(wtr.into_inner()?)
}
