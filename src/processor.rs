use anyhow::{Context, Result};
use csv::ReaderBuilder;
use rayon::prelude::*;
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Deserialize, Clone)]
pub struct Record {
    #[serde(rename = "Timestamp")]
    #[allow(dead_code)]
    pub timestamp: String,

    #[serde(rename = "SensorID")]
    pub sensor_id: String,

    #[serde(rename = "Value")]
    pub value: f64,
}

#[derive(Debug)]
pub struct ProcessingStats {
    pub total_rows: usize,
    pub filtered_rows: usize,
    pub average: Option<f64>,
    #[allow(dead_code)]
    pub per_sensor: Vec<SensorStats>,
}

#[derive(Debug)]
pub struct SensorStats {
    pub sensor_id: String,
    pub count: usize,
    pub average: f64,
}

#[derive(Default, Clone)]
struct Accumulator {
    count: usize,
    sum: f64,
}

impl Accumulator {
    fn add(&mut self, value: f64) {
        self.count += 1;
        self.sum += value;
    }

    fn merge(mut self, other: Self) -> Self {
        self.count += other.count;
        self.sum += other.sum;
        self
    }
}

pub fn process(path: &Path, threshold: f64, verbose: bool) -> Result<ProcessingStats> {
    let records = read_csv(path)?;
    let total_rows = records.len();

    let global_acc = records
        .par_iter()
        .filter(|r| r.value > threshold)
        .fold(Accumulator::default, |mut acc, r| {
            acc.add(r.value);
            acc
        })
        .reduce(Accumulator::default, Accumulator::merge);

    let filtered_rows = global_acc.count;
    let average = if global_acc.count > 0 {
        Some(global_acc.sum / global_acc.count as f64)
    } else {
        None
    };

    let per_sensor = if verbose {
        compute_per_sensor_stats(&records, threshold)
    } else {
        Vec::new()
    };

    if verbose && !per_sensor.is_empty() {
        print_sensor_table(&per_sensor);
    }

    Ok(ProcessingStats {
        total_rows,
        filtered_rows,
        average,
        per_sensor,
    })
}

fn read_csv(path: &Path) -> Result<Vec<Record>> {
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .trim(csv::Trim::All)
        .from_path(path)
        .with_context(|| format!("Cannot open CSV file '{}'", path.display()))?;

    let records: Result<Vec<Record>, _> = reader.deserialize().collect();

    records.with_context(|| {
        format!("Failed to deserialize one or more rows in '{}'", path.display())
    })
}

fn compute_per_sensor_stats(records: &[Record], threshold: f64) -> Vec<SensorStats> {
    use std::collections::HashMap;
    use std::sync::Mutex;

    let map: Mutex<HashMap<String, Accumulator>> = Mutex::new(HashMap::new());

    records
        .par_iter()
        .filter(|r| r.value > threshold)
        .for_each(|r| {
            let mut guard = map.lock().expect("mutex poisoned");
            guard.entry(r.sensor_id.clone()).or_default().add(r.value);
        });

    let mut stats: Vec<SensorStats> = map
        .into_inner()
        .expect("mutex poisoned")
        .into_iter()
        .map(|(sensor_id, acc)| SensorStats {
            sensor_id,
            count: acc.count,
            average: acc.sum / acc.count as f64,
        })
        .collect();

    stats.sort_unstable_by(|a, b| a.sensor_id.cmp(&b.sensor_id));
    stats
}

fn print_sensor_table(stats: &[SensorStats]) {
    println!();
    println!("  {:<20} {:>10} {:>16}", "Sensor ID", "Row Count", "Average Value");
    println!("  {:-<20} {:->10} {:->16}", "", "", "");
    for s in stats {
        println!("  {:<20} {:>10} {:>16.6}", s.sensor_id, s.count, s.average);
    }
    println!();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn make_temp_csv(content: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().expect("tmp file");
        write!(f, "{content}").expect("write");
        f
    }

    #[test]
    fn test_filter_and_average() {
        let csv = "\
Timestamp,SensorID,Value
2024-01-01T00:00:00,S1,10.0
2024-01-01T00:00:01,S2,60.0
2024-01-01T00:00:02,S1,80.0
2024-01-01T00:00:03,S3,30.0
";
        let file = make_temp_csv(csv);
        let stats = process(file.path(), 50.0, false).expect("process");

        assert_eq!(stats.total_rows, 4);
        assert_eq!(stats.filtered_rows, 2);
        let avg = stats.average.expect("average should be Some");
        assert!((avg - 70.0).abs() < 1e-9, "expected 70.0, got {avg}");
    }

    #[test]
    fn test_no_rows_pass_filter() {
        let csv = "\
Timestamp,SensorID,Value
2024-01-01T00:00:00,S1,1.0
2024-01-01T00:00:01,S2,2.0
";
        let file = make_temp_csv(csv);
        let stats = process(file.path(), 100.0, false).expect("process");

        assert_eq!(stats.filtered_rows, 0);
        assert!(stats.average.is_none());
    }

    #[test]
    fn test_all_rows_pass_filter() {
        let csv = "\
Timestamp,SensorID,Value
2024-01-01T00:00:00,S1,100.0
2024-01-01T00:00:01,S2,200.0
";
        let file = make_temp_csv(csv);
        let stats = process(file.path(), 0.0, false).expect("process");

        assert_eq!(stats.total_rows, 2);
        assert_eq!(stats.filtered_rows, 2);
        let avg = stats.average.unwrap();
        assert!((avg - 150.0).abs() < 1e-9);
    }

    #[test]
    fn test_verbose_per_sensor() {
        let csv = "\
Timestamp,SensorID,Value
2024-01-01T00:00:00,S1,60.0
2024-01-01T00:00:01,S1,80.0
2024-01-01T00:00:02,S2,90.0
2024-01-01T00:00:03,S2,10.0
";
        let file = make_temp_csv(csv);
        let stats = process(file.path(), 50.0, true).expect("process");

        assert_eq!(stats.per_sensor.len(), 2);
        let s1 = stats.per_sensor.iter().find(|s| s.sensor_id == "S1").unwrap();
        assert_eq!(s1.count, 2);
        assert!((s1.average - 70.0).abs() < 1e-9);
        let s2 = stats.per_sensor.iter().find(|s| s.sensor_id == "S2").unwrap();
        assert_eq!(s2.count, 1);
        assert!((s2.average - 90.0).abs() < 1e-9);
    }
}
