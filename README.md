
A CLI tool that reads large CSV files of biometric data, filters rows by a value threshold, and computes the average, using parallel processing via **rayon**.

## CSV Format

```
Timestamp,SensorID,Value
2024-01-01T00:00:00,SENSOR_001,73.42
2024-01-01T00:00:01,SENSOR_002,28.11
```

## Build

```bash
cargo build --release
```

## Usage

```bash
./target/release/rust-cli --input data.csv --filter-threshold 50.0
```

| Flag | Default | Description |
|---|---|---|
| `--input` / `-i` | required | Path to the CSV file |
| `--filter-threshold` / `-t` | `0.0` | Keep rows where `Value > threshold` |
| `--verbose` / `-v` | off | Print per-sensor statistics table |

## Example Output

```
Input file      : data.csv
Filter threshold: 50
Threads (rayon) : 12

Processing complete
    Total rows read      : 1000000
    Rows after filter    : 499837
    Rows removed         : 500163 (50.02%)
    Average value        : 74.985312
Wall-clock time : 312.543ms
```

## Run Tests

```bash
cargo test
```

## Benchmark (Rust vs Pandas)

```bash
pip install pandas numpy
python benchmark.py
```

The script generates a 1-million-row CSV, runs the same filter+average operation in Pandas, then runs the Rust binary, and prints a side-by-side comparison with a speed-up multiplier.

Optional flags:

```bash
python benchmark.py --rows 2000000 --threshold 30.0 --csv my_data.csv
python benchmark.py --skip-generate --csv existing_data.csv
```

## Project Structure

```
Rust-CLI/
├── src/
│   ├── main.rs        # CLI argument parsing (clap)
│   └── processor.rs   # CSV reading, parallel filter+average (rayon)
├── benchmark.py       # Pandas vs Rust benchmark
└── Cargo.toml
```

## Dependencies

| Crate | Purpose |
|---|---|
| `clap` | CLI argument parsing |
| `csv` | CSV reading |
| `serde` | Deserialization into `Record` structs |
| `rayon` | Data-parallel iterators |
| `anyhow` | Ergonomic error handling |
