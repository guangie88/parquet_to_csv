extern crate csv;
extern crate parquet;

use self::csv::Writer;
use self::parquet::column::reader::ColumnReader;
use self::parquet::data_type::Int96;
use self::parquet::errors::ParquetError;
use self::parquet::file::reader::{FileReader, SerializedFileReader};
use std::error::Error;
use std::fs::File;
use std::io::Write;

macro_rules! read_format {
    ( $v:expr, $d:expr ) => {
        {
            let mut vs = vec![$d; 1];
            $v.read_batch(1, None, None, &mut vs).unwrap();
            format!("{:?}", vs[0])
        }
    };
}

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
                ColumnReader::BoolColumnReader(mut v) => read_format!(v, false),
                ColumnReader::Int32ColumnReader(mut v) => read_format!(v, 0),
                ColumnReader::Int64ColumnReader(mut v) => read_format!(v, 0),
                ColumnReader::Int96ColumnReader(mut v) => read_format!(v, Int96::new()),
                ColumnReader::FloatColumnReader(mut v) => read_format!(v, 0.0),
                ColumnReader::DoubleColumnReader(mut v) => read_format!(v, 0.0),
                ColumnReader::ByteArrayColumnReader(_) => "byte_array".to_owned(),
                ColumnReader::FixedLenByteArrayColumnReader(_) => {
                    "fixed_len_byte_array".to_owned()
                }
            });

        wtr.write_record(col_values)?
    }

    Ok(wtr.into_inner()?)
}
