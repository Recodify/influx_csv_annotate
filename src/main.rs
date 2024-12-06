use chrono::{DateTime, Duration, Utc};
use csv::{ReaderBuilder, WriterBuilder};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fs::File;
use std::io::{BufWriter, Write};

#[derive(Debug, Deserialize)]
struct InputRow {
    #[serde(rename = "readingDate")]
    reading_date: String,
    value: f64,
    #[serde(rename = "displayReference")]
    display_reference: String,
    location: String,
    #[serde(rename = "readingType")]
    reading_type: String,
}

#[derive(Debug, Serialize)]
struct OutputRow {
    result: String,
    table: String,
    _start: String,
    _stop: String,
    _time: String,
    _value: String,
    _field: String,
    _measurement: String,
    display_reference: String,
    location: String,
    reading_type: String,
}

fn process_chunk(chunk: &[InputRow], now: DateTime<Utc>) -> Result<Vec<OutputRow>, Box<dyn Error>> {
    if chunk.is_empty() {
        return Ok(Vec::new());
    }

    let mut min_date = DateTime::parse_from_rfc3339(&chunk[0].reading_date)?.with_timezone(&Utc);
    let mut max_date = min_date;
    let mut dates = Vec::with_capacity(chunk.len());

    for row in chunk {
        let date = DateTime::parse_from_rfc3339(&row.reading_date)?.with_timezone(&Utc);
        if date < min_date {
            min_date = date;
        }
        if date > max_date {
            max_date = date;
        }
        dates.push(date);
    }

    let duration = (max_date - min_date).num_seconds() as f64;

    let mut output_rows = Vec::with_capacity(chunk.len());
    for (i, row) in chunk.iter().enumerate() {
        let reading_date = if duration > 0.0 {
            let seconds_from_min = (dates[i] - min_date).num_seconds() as f64;
            let total_seconds = 15.0 * 86400.0 * (seconds_from_min / duration);
            now - Duration::days(20) + Duration::seconds(total_seconds as i64)
        } else {
            dates[i]
        };

        let display_reference = if row.display_reference.contains(',') || row.display_reference.contains(' ') {
            format!("\"{}\"", row.display_reference)
        } else {
            row.display_reference.clone()
        };

        let location = if row.location.contains(',') || row.location.contains(' ') {
            format!("\"{}\"", row.location)
        } else {
            row.location.clone()
        };

        let rfc3339_date = reading_date.to_rfc3339();

        output_rows.push(OutputRow {
            result: String::new(),
            table: String::new(),
            _start: rfc3339_date.clone(),
            _stop: rfc3339_date.clone(),
            _time: rfc3339_date,
            _value: row.value.to_string(),
            _field: "reading".to_string(),
            _measurement: "metrics".to_string(),
            display_reference,
            location,
            reading_type: row.reading_type.clone(),
        });
    }

    Ok(output_rows)
}

fn process_and_write_chunk(
    chunk: Vec<InputRow>,
    annotations: &[&str],
    output_file: &str,
    now: DateTime<Utc>,
) -> Result<(), Box<dyn Error>> {
    let processed_rows = process_chunk(&chunk, now)?;

    let file = File::create(output_file)?;
    let mut writer = BufWriter::new(file);

    for annotation in annotations {
        writeln!(writer, "{}", annotation)?;
    }

    let mut csv_writer = WriterBuilder::new().has_headers(false).from_writer(writer);
    for row in processed_rows {
        csv_writer.serialize(row)?;
    }

    Ok(())
}

fn transform_chunks(input_file: &str, chunk_size: usize) -> Result<(), Box<dyn Error>> {
    let now = Utc::now();
    println!("Processing CSV in chunks...");

    let annotations = [
        "#group,false,false,true,true,false,false,true,true,true,true,true",
        "#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string",
        "#default,mean,,,,,,,,,,",
        ",result,table,_start,_stop,_time,_value,_field,_measurement,displayReference,location,readingType",
    ];

    let file = File::open(input_file)?;
    let mut reader = ReaderBuilder::new().has_headers(true).from_reader(file);

    let mut chunks = Vec::new();
    let mut current_chunk = Vec::with_capacity(chunk_size);

    for result in reader.deserialize() {
        let record: InputRow = result?;
        current_chunk.push(record);

        if current_chunk.len() >= chunk_size {
            chunks.push(current_chunk);
            current_chunk = Vec::with_capacity(chunk_size);
        }
    }

    if !current_chunk.is_empty() {
        chunks.push(current_chunk);
    }

    chunks.into_par_iter().enumerate().for_each(|(chunk_num, chunk)| {
        let output_file = format!("influx_batch_{}.csv", chunk_num);
        process_and_write_chunk(chunk, &annotations, &output_file, now)
            .expect("Error processing chunk");
    });

    println!("Transformation complete!");
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let input_file = "readings.csv";
    transform_chunks(input_file, 210000)?;
    Ok(())
}
