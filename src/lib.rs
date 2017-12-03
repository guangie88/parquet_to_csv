extern crate csv;
extern crate parquet;

use self::csv::Writer;
use self::parquet::column::reader::ColumnReader;
use self::parquet::errors::ParquetError;
use self::parquet::file::reader::{FileReader, SerializedFileReader};
use std::error::Error;
use std::fs::File;
use std::io::Write;

pub fn parquet_to_csv<W>(file: File, w: W) -> Result<W, Box<Error>>
where
    W: Write + 'static,
{
    let reader = SerializedFileReader::new(file)?;
    let row_count = reader.num_row_groups();

    let row_groups = (0..row_count)
        .map(|r| reader.get_row_group(r))
        .collect::<Result<Vec<_>, ParquetError>>()?;

    let mut wtr = Writer::from_writer(w);

    for row_reader in row_groups {
        let col_count = row_reader.num_columns();

        let col_values = (0..col_count)
            .map(|c| row_reader.get_column_reader(c))
            .collect::<Result<Vec<_>, ParquetError>>()?
            .into_iter()
            .map(|col_reader| match col_reader {
                ColumnReader::BoolColumnReader(v) => "bool".to_owned(),
                ColumnReader::Int32ColumnReader(mut v) => {
                    let mut vs = vec![0; 1];
                    v.read_batch(1, None, None, &mut vs).unwrap();
                    format!("{}", vs[0])
                }
                ColumnReader::Int64ColumnReader(v) => "int64".to_owned(),
                ColumnReader::Int96ColumnReader(v) => "int96".to_owned(),
                ColumnReader::FloatColumnReader(v) => "float".to_owned(),
                ColumnReader::DoubleColumnReader(v) => "double".to_owned(),
                ColumnReader::ByteArrayColumnReader(v) => "byte_array".to_owned(),
                ColumnReader::FixedLenByteArrayColumnReader(v) => "fixed_len_byte_array".to_owned(),
            });

        wtr.write_record(col_values)?
    }

    Ok(wtr.into_inner()?)
}
