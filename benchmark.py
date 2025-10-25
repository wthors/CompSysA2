#!/usr/bin/env python3
"""
Benchmark script for CompSys A2 programs.

This script automates running the single‑threaded and multi‑threaded
versions of the fauxgrep and fhistogram programs and computes
throughput metrics.  It is designed to help fulfil the benchmarking
requirements described in the assignment specification【956840366162655†L117-L195】.

Usage:

    python3 benchmark.py --data PATH [--needle STRING] [--max‑threads N]

Options:

    --data PATH         Path to the directory tree to use for benchmarking.
                        This directory should contain a reasonably large
                        number of files to make multithreading worthwhile.
    --needle STRING     Substring to search for when running fauxgrep.  If
                        omitted, a default value of "TODO" is used.  The
                        specific value is not important so long as it is
                        representative of typical searches.
    --max‑threads N     Maximum number of worker threads to test when
                        running the multi‑threaded programs.  The script
                        will benchmark with 1, 2, 4, … up to N threads.  If
                        omitted, the default is 8.

The binaries fauxgrep, fauxgrep‑mt, fhistogram and fhistogram‑mt must
already be built in the current working directory.  You can compile
them by running `make` in the src directory of the assignment.

Example:

    # compile the programs
    cd src && make && cd ..
    # run the benchmark on the src directory using up to 8 threads
    python3 benchmark.py --data src --needle malloc --max‑threads 8

The script prints a summary table of the running time as well as the
throughput measured in files per second and bytes per second.  It can
be extended or customised further as needed.
"""

import argparse
import os
import subprocess
import time
from typing import List, Tuple


def count_files_and_bytes(root: str) -> Tuple[int, int]:
    """Recursively count the number of files and total bytes under a directory."""
    total_files = 0
    total_bytes = 0
    for dirpath, _, filenames in os.walk(root):
        for fname in filenames:
            fpath = os.path.join(dirpath, fname)
            try:
                total_files += 1
                total_bytes += os.path.getsize(fpath)
            except OSError:
                # Skip files that cannot be accessed
                continue
    return total_files, total_bytes


def run_command(cmd: List[str]) -> float:
    """Run a command and return the elapsed wall‑clock time in seconds."""
    start = time.perf_counter()
    # Redirect stdout/stderr to devnull to avoid skewing timing with I/O
    with open(os.devnull, "wb") as devnull:
        subprocess.run(cmd, stdout=devnull, stderr=devnull, check=True)
    end = time.perf_counter()
    return end - start


def benchmark_programs(data_dir: str, needle: str, max_threads: int) -> None:
    """Run benchmarks for fauxgrep and fhistogram variants and print results."""
    total_files, total_bytes = count_files_and_bytes(data_dir)
    print(f"Data set: {data_dir}")
    print(f"Total files: {total_files}")
    print(f"Total bytes: {total_bytes}")
    print()

    thread_counts = [1]
    # Build list of thread counts: 1, 2, 4, … up to max_threads
    t = 1
    while t < max_threads:
        t *= 2
        thread_counts.append(t)

    results: List[Tuple[str, int, float, float, float]] = []

    # Benchmark fauxgrep (single threaded)
    try:
        duration = run_command(["./fauxgrep", needle, data_dir])
        results.append(("fauxgrep", 1, duration, total_files / duration, total_bytes / duration))
    except FileNotFoundError:
        print("Warning: fauxgrep binary not found; skipping.")

    # Benchmark fauxgrep‑mt for various thread counts
    for n in thread_counts:
        try:
            duration = run_command(["./fauxgrep-mt", "-n", str(n), needle, data_dir])
            results.append(("fauxgrep‑mt", n, duration, total_files / duration, total_bytes / duration))
        except FileNotFoundError:
            print("Warning: fauxgrep‑mt binary not found; skipping.")
            break

    # Benchmark fhistogram (single threaded)
    try:
        duration = run_command(["./fhistogram", data_dir])
        results.append(("fhistogram", 1, duration, total_files / duration, total_bytes / duration))
    except FileNotFoundError:
        print("Warning: fhistogram binary not found; skipping.")

    # Benchmark fhistogram‑mt
    for n in thread_counts:
        try:
            duration = run_command(["./fhistogram-mt", "-n", str(n), data_dir])
            results.append(("fhistogram‑mt", n, duration, total_files / duration, total_bytes / duration))
        except FileNotFoundError:
            print("Warning: fhistogram‑mt binary not found; skipping.")
            break

    # Print results in a table
    if results:
        print(f"{'Program':<15}{'Threads':<10}{'Time(s)':<12}{'Files/s':<15}{'Bytes/s'}")
        print("-" * 64)
        for prog, n_threads, time_s, files_per_s, bytes_per_s in results:
            print(f"{prog:<15}{n_threads:<10}{time_s:<12.3f}{files_per_s:<15.1f}{bytes_per_s:.1f}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark fauxgrep and fhistogram variants.")
    parser.add_argument("--data", required=True, help="Directory containing test data")
    parser.add_argument("--needle", default="TODO", help="Search string for fauxgrep")
    # Note: use an ASCII hyphen in the flag name and set a valid dest name.
    parser.add_argument(
        "--max-threads",
        type=int,
        dest="max_threads",
        default=8,
        help="Maximum worker threads to test",
    )
    args = parser.parse_args()

    benchmark_programs(args.data, args.needle, args.max_threads)


if __name__ == "__main__":
    main()