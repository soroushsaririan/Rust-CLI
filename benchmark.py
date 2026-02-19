#!/usr/bin/env python3
"""
Generate a 1-million-row biometric CSV and benchmark Rust CLI vs Pandas.

Usage:
    python benchmark.py [--rows 1_000_000] [--threshold 50.0] [--csv data.csv]

Requirements:
    pip install pandas numpy
"""

import argparse
import subprocess
import sys
import time
import csv as stdlib_csv
from pathlib import Path

try:
    import pandas as pd
    import numpy as np
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

GREEN  = "\033[32m"
YELLOW = "\033[33m"
CYAN   = "\033[36m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

def bold(s):   return f"{BOLD}{s}{RESET}"
def green(s):  return f"{GREEN}{s}{RESET}"
def yellow(s): return f"{YELLOW}{s}{RESET}"
def cyan(s):   return f"{CYAN}{s}{RESET}"


def generate_csv(path: Path, rows: int) -> None:
    rng = np.random.default_rng(seed=42)
    sensors = [f"SENSOR_{i:03d}" for i in range(1, 51)]
    base_ts = 1_704_067_200
    timestamps = [time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(base_ts + i)) for i in range(rows)]
    sensor_ids = rng.choice(sensors, size=rows)
    values = rng.uniform(0.0, 100.0, size=rows).round(4)

    print(f"  Writing {rows:,} rows to '{path}' …", end=" ", flush=True)
    t0 = time.perf_counter()
    with open(path, "w", newline="") as f:
        writer = stdlib_csv.writer(f)
        writer.writerow(["Timestamp", "SensorID", "Value"])
        writer.writerows(zip(timestamps, sensor_ids, values))
    elapsed = time.perf_counter() - t0
    size_mb = path.stat().st_size / 1_048_576
    print(f"done  ({elapsed:.2f}s, {size_mb:.1f} MB)")


def run_pandas(csv_path: Path, threshold: float) -> dict:
    if not HAS_PANDAS:
        return {"error": "pandas/numpy not installed"}

    t_start = time.perf_counter()
    df = pd.read_csv(csv_path)
    t_read = time.perf_counter()
    total_rows = len(df)
    filtered = df[df["Value"] > threshold]
    t_filter = time.perf_counter()
    average = filtered["Value"].mean() if len(filtered) > 0 else float("nan")
    t_avg = time.perf_counter()

    return {
        "total_rows":    total_rows,
        "filtered_rows": len(filtered),
        "average":       average,
        "read_s":        t_read   - t_start,
        "filter_s":      t_filter - t_read,
        "avg_s":         t_avg    - t_filter,
        "total_s":       t_avg    - t_start,
    }


def find_rust_binary(workspace: Path):
    for name in ("rust-cli", "rust_cli"):
        p = workspace / "target" / "release" / name
        if p.exists():
            return p
    return None


def run_rust(csv_path: Path, threshold: float, workspace: Path) -> dict:
    binary = find_rust_binary(workspace)
    if binary is None:
        return {"error": f"Rust binary not found – run `cargo build --release` first.\n  Looked in: {workspace}/target/release/"}

    cmd = [str(binary), "--input", str(csv_path), "--filter-threshold", str(threshold)]
    t_start = time.perf_counter()
    result = subprocess.run(cmd, capture_output=True, text=True)
    total_s = time.perf_counter() - t_start

    if result.returncode != 0:
        return {"error": f"Rust process exited with code {result.returncode}:\n{result.stderr}"}

    stats: dict = {"total_s": total_s}
    for line in result.stdout.splitlines():
        line = line.strip()
        if "Total rows read" in line:
            stats["total_rows"] = int(line.split(":")[-1].strip().replace(",", ""))
        elif "Rows after filter" in line:
            stats["filtered_rows"] = int(line.split(":")[-1].strip().replace(",", ""))
        elif "Average value" in line:
            val = line.split(":")[-1].strip()
            stats["average"] = float(val) if val != "N/A" else float("nan")
        elif "Wall-clock time" in line:
            stats["rust_wall"] = line.split(":")[-1].strip()
    return stats


def print_report(pandas_r: dict, rust_r: dict, rows: int, threshold: float) -> None:
    print()
    print(bold("═" * 62))
    print(bold("  BENCHMARK RESULTS"))
    print(bold("═" * 62))
    print(f"  Rows generated : {rows:>12,}")
    print(f"  Threshold      : {threshold:>12.2f}")
    print(bold("─" * 62))

    print(cyan(bold("\n  [Pandas]")))
    if "error" in pandas_r:
        print(f"  {yellow(pandas_r['error'])}")
    else:
        print(f"  Total rows       : {pandas_r['total_rows']:>12,}")
        print(f"  Filtered rows    : {pandas_r['filtered_rows']:>12,}")
        print(f"  Average value    : {pandas_r['average']:>16.6f}")
        print(f"  Read time        : {pandas_r['read_s']:>12.4f} s")
        print(f"  Filter time      : {pandas_r['filter_s']:>12.4f} s")
        print(f"  Avg compute time : {pandas_r['avg_s']:>12.4f} s")
        print(f"  {bold('Total time')}       : {bold(f'{pandas_r[\"total_s\"]:>12.4f} s')}")

    print(green(bold("\n  [Rust CLI]")))
    if "error" in rust_r:
        print(f"  {yellow(rust_r['error'])}")
    else:
        print(f"  Total rows       : {rust_r.get('total_rows', 'N/A'):>12,}")
        print(f"  Filtered rows    : {rust_r.get('filtered_rows', 'N/A'):>12,}")
        avg = rust_r.get("average", float("nan"))
        if avg != avg:
            print(f"  Average value    : {'N/A':>16}")
        else:
            print(f"  Average value    : {avg:>16.6f}")
        print(f"  Internal time    : {rust_r.get('rust_wall', '?'):>16}")
        print(f"  {bold('Total time')}       : {bold(f'{rust_r[\"total_s\"]:>12.4f} s')}")

    print(bold("\n  [Speed-up]"))
    if "error" not in pandas_r and "error" not in rust_r:
        speedup = pandas_r["total_s"] / rust_r["total_s"]
        print(f"  Rust is {bold(green(f'{speedup:.2f}×'))} faster than Pandas")
        print(f"  {green('█' * min(int(speedup), 60))}")
    else:
        print("  (Cannot compute: one or both runs failed)")

    print()
    print(bold("═" * 62))
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark Rust CLI vs Pandas on a large biometric CSV")
    parser.add_argument("--rows",          type=int,   default=1_000_000, help="Rows to generate (default: 1,000,000)")
    parser.add_argument("--threshold",     type=float, default=50.0,      help="Filter threshold (default: 50.0)")
    parser.add_argument("--csv",           type=Path,  default=Path("data.csv"), help="CSV file path (default: data.csv)")
    parser.add_argument("--skip-generate", action="store_true",           help="Skip CSV generation and use existing file")
    args = parser.parse_args()

    workspace = Path(__file__).resolve().parent

    print()
    print(bold("╔══════════════════════════════════════════════════════════╗"))
    print(bold("║       Rust CLI  vs  Pandas  –  Biometric CSV Benchmark   ║"))
    print(bold("╚══════════════════════════════════════════════════════════╝"))
    print()

    if not args.skip_generate:
        if not HAS_PANDAS:
            print("WARNING: pandas/numpy not found.  Install them:  pip install pandas numpy")
            sys.exit(1)
        print(bold("Step 1 – Generating CSV"))
        generate_csv(args.csv, args.rows)
    else:
        if not args.csv.exists():
            print(f"Error: '{args.csv}' not found.", file=sys.stderr)
            sys.exit(1)
        print(f"  Using existing CSV: {args.csv}")

    print()
    print(bold("Step 2 – Running Pandas benchmark …"))
    pandas_result = run_pandas(args.csv, args.threshold)

    print()
    print(bold("Step 3 – Running Rust CLI benchmark …"))
    rust_result = run_rust(args.csv, args.threshold, workspace)

    print_report(pandas_result, rust_result, args.rows, args.threshold)


if __name__ == "__main__":
    main()
