use anyhow::{Context, Result};
use clap::Parser;
use std::path::PathBuf;

mod processor;

#[derive(Parser, Debug)]
#[command(
    name = "rust-cli",
    version,
    author,
    about = "Process large biometric CSV files at blazing speed using parallel execution"
)]
struct Cli {
    /// Path to the input CSV file (Timestamp, SensorID, Value)
    #[arg(short, long, value_name = "FILE")]
    input: PathBuf,

    /// Keep only rows where Value > threshold
    #[arg(short = 't', long, value_name = "FLOAT", default_value_t = 0.0)]
    filter_threshold: f64,

    /// Print per-sensor statistics after processing
    #[arg(short, long, default_value_t = false)]
    verbose: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    if !cli.input.exists() {
        anyhow::bail!("Input file '{}' does not exist.", cli.input.display());
    }
    if !cli.input.is_file() {
        anyhow::bail!("'{}' is not a regular file.", cli.input.display());
    }

    println!("Input file      : {}", cli.input.display());
    println!("Filter threshold: {}", cli.filter_threshold);
    println!("Threads (rayon) : {}", rayon::current_num_threads());
    println!();

    let start = std::time::Instant::now();

    let stats = processor::process(&cli.input, cli.filter_threshold, cli.verbose)
        .with_context(|| format!("Failed to process file '{}'", cli.input.display()))?;

    let elapsed = start.elapsed();

    println!("Processing complete");
    println!("    Total rows read      : {}", stats.total_rows);
    println!("    Rows after filter    : {}", stats.filtered_rows);
    println!(
        "    Rows removed         : {} ({:.2}%)",
        stats.total_rows - stats.filtered_rows,
        if stats.total_rows > 0 {
            (stats.total_rows - stats.filtered_rows) as f64 / stats.total_rows as f64 * 100.0
        } else {
            0.0
        }
    );

    match stats.average {
        Some(avg) => println!("    Average value        : {:.6}", avg),
        None => println!("    Average value        : N/A (no rows passed the filter)"),
    }

    println!("Wall-clock time : {:.4?}", elapsed);

    Ok(())
}
