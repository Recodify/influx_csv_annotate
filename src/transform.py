import pandas as pd
import sys
from datetime import datetime, timedelta
import io

def process_chunk(chunk, now):
    chunk['readingDate'] = pd.to_datetime(chunk['readingDate']).dt.tz_localize(None)
    min_date = chunk['readingDate'].min()
    max_date = chunk['readingDate'].max()
    duration = (max_date - min_date).total_seconds()

    if duration > 0:
        chunk['readingDate'] = chunk.apply(lambda row:
            now - timedelta(days=20) +
            timedelta(days=15) * (row['readingDate'] - min_date).total_seconds() / duration,
            axis=1)

    chunk['_start'] = chunk['readingDate']
    chunk['_stop'] = chunk['readingDate']
    return chunk

def transform_chunks(input_file, chunksize=10000):
    now = datetime.utcnow()
    print("Processing CSV in chunks...")

    annotations = [
        "#group,false,false,true,true,false,false,true,true,true,true,true",
        "#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string",
        "#default,mean,,,,,,,,,,",
        ",result,table,_start,_stop,_time,_value,_field,_measurement,displayReference,location,readingType"
    ]

    total_rows = 0
    for chunk_num, chunk in enumerate(pd.read_csv(input_file, chunksize=chunksize)):
        output_file = f'influx4_batch_{chunk_num}.csv'
        processed_chunk = process_chunk(chunk, now)
        rows = []

        for _, row in processed_chunk.iterrows():
            output_row = [
                "",
                "",
                "0",
                row['_start'].strftime("%Y-%m-%dT%H:%M:%SZ"),
                row['_stop'].strftime("%Y-%m-%dT%H:%M:%SZ"),
                row['readingDate'].strftime("%Y-%m-%dT%H:%M:%SZ"),
                str(row['value']),
                "reading",
                "metrics",
                f'"{row["displayReference"]}"' if ',' in str(row["displayReference"]) or ' ' in str(row["displayReference"]) else str(row["displayReference"]),
                f'"{row["location"]}"' if ',' in str(row["location"]) or ' ' in str(row["location"]) else str(row["location"]),
                row['readingType']
            ]
            rows.append(",".join(output_row))

        total_rows += len(rows)
        with open(output_file, 'w') as f:
            f.write('\n'.join(annotations) + '\n')
            f.write('\n'.join(rows) + '\n')

        print(f"Processed chunk {chunk_num + 1}, rows: {len(rows)}, total: {total_rows}, file: {output_file}")

if __name__ == "__main__":
    try:
        input_file = 'READING_2023-06-01.csv'
        transform_chunks(input_file, chunksize=210000)
        print("Transformation complete!")
    except Exception as e:
        print(f"Error occurred: {str(e)}", file=sys.stderr)
        raise