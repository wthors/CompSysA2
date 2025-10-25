#!/usr/bin/env python3
"""
Generate a synthetic dataset for benchmarking fauxgrep and fhistogram.

This script can either replicate an existing directory tree multiple
times or generate a specified number of random text files.  It
defaults to copying the project's `src` directory ten times into a
destination directory.  Use command‐line options to customise the
behaviour.

Examples:

    # Copy the src directory 50 times into /tmp/dataset
    python3 create_dataset.py --dest /tmp/dataset --copies 50

    # Generate 1000 random files of 4KB each
    python3 create_dataset.py --mode random --dest /tmp/dataset \
        --num-files 1000 --file-size 4096

Run with -h for full usage information.
"""

import argparse
import os
import random
import shutil
import string
import sys


def make_copy_dataset(source: str, dest: str, copies: int) -> None:
    """Replicate the contents of `source` directory into `dest` N times."""
    if not os.path.isdir(source):
        raise ValueError(f"source directory '{source}' does not exist")
    os.makedirs(dest, exist_ok=True)
    for i in range(copies):
        copy_dest = os.path.join(dest, f"copy_{i}")
        if os.path.exists(copy_dest):
            shutil.rmtree(copy_dest)
        print(f"Copying '{source}' to '{copy_dest}'")
        shutil.copytree(source, copy_dest)
    print(f"Created dataset at '{dest}' with {copies} copies of '{source}'")


def make_random_dataset(dest: str, num_files: int, file_size: int) -> None:
    """Generate `num_files` random text files of `file_size` bytes each."""
    os.makedirs(dest, exist_ok=True)
    alphabet = string.ascii_letters + string.digits + ' \n'
    for i in range(num_files):
        filename = os.path.join(dest, f"file_{i:06d}.txt")
        print(f"Creating random file '{filename}'")
        with open(filename, "w", encoding="utf-8") as f:
            remaining = file_size
            while remaining > 0:
                # Write up to 80 chars per line
                line_len = min(80, remaining)
                line = ''.join(random.choices(alphabet, k=line_len))
                f.write(line)
                f.write("\n")
                remaining -= line_len + 1  # account for newline
    print(f"Created dataset at '{dest}' with {num_files} random files")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a dataset by copying a directory or generating random files",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--mode",
        choices=["copy", "random"],
        default="copy",
        help="Dataset generation mode: copy a directory or create random files",
    )
    parser.add_argument(
        "--source",
        default="src",
        help="Source directory to copy in copy mode",
    )
    parser.add_argument(
        "--copies",
        type=int,
        default=10,
        help="Number of copies to make in copy mode",
    )
    parser.add_argument(
        "--dest",
        required=True,
        help="Destination directory for the generated dataset",
    )
    parser.add_argument(
        "--num-files",
        type=int,
        default=100,
        help="Number of files to generate in random mode",
    )
    parser.add_argument(
        "--file-size",
        type=int,
        default=1024,
        help="Size of each random file in bytes",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.mode == "copy":
        make_copy_dataset(args.source, args.dest, args.copies)
    elif args.mode == "random":
        make_random_dataset(args.dest, args.num_files, args.file_size)
    else:
        raise ValueError(f"Unknown mode {args.mode}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)