# Influx_csv_annotate

Annotate a simple CSV (with headers) for influx import.

## Notes

Currently for a fix csv file (known headers), will updated to handle any csv in a subsequent version.


## Versions

#### Node.js

npm i
cd ./src
code transform.js


#### Python (3.0!)

python transform.py

#### Rust

cargo build --release
./target/release/transform

## Perf

On a 147.8mb file contain 1105586 data rows.

#### Python v3.8.10

`real    2m3.236s`
`user    2m2.463s`
`sys     0m1.137s`

Why so slow?

#### Node.js v20.18.0

`real    0m7.700s`
`user    0m9.627s`
`sys     0m1.190s`

decent...

#### Rust v1.83.0

`real    0m1.948s`
`user    0m3.917s`
`sys     0m1.703s`

...go Rust!