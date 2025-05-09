#!/usr/bin/env python3
import os
import subprocess
import csv
import re
import argparse
import sys
from datetime import datetime

def run_benchmark(command):
    """
    Runs the given shell command, captures its stdout,
    and extracts benchmark statistics.
    """
    proc = subprocess.run(command, shell=True,
                          stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT,
                          universal_newlines=True)
    out = proc.stdout

    # regexes for the values we care about
    patterns = {
        'operations': re.compile(r"# operations:\s*(\d+)"),
        'elapsed_ms': re.compile(r"Elapsed Time:\s*(\d+)"),
        'ops_per_sec': re.compile(r"Ops per sec:\s*([\d\.]+)"),
        'avg_time_ms': re.compile(r"Average Time:\s*(\d+)")
    }

    stats = {}
    for key, pat in patterns.items():
        m = pat.search(out)
        stats[key] = m.group(1) if m else None

    return stats

def main():
    p = argparse.ArgumentParser(
        description="Run HDFS NNThroughputBenchmark multiple times and log stats to CSV."
    )
    p.add_argument('-n', '--runs',      type=int,   default=1,
                   help="Number of times to run the benchmark")
    p.add_argument('--threads',         type=int,   default=100,
                   help="Value for -threads")
    p.add_argument('--files',           type=int,   default=100000,
                   help="Value for -files")
    p.add_argument('--fs',              type=str,   default='hdfs://localhost:9000',
                   help="Filesystem URI for -fs")
    p.add_argument('--op',              type=str,   default='create',
                   help="Operation for -op (default: create)")
    args = p.parse_args()

    # 1) Read HADOOP_HOME from the environment
    hadoop_home = os.environ.get('HADOOP_HOME')
    if not hadoop_home:
        print("ERROR: environment variable HADOOP_HOME is not set.", file=sys.stderr)
        sys.exit(1)

    # Generate filename: <timestamp>_<operation>.csv
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_file = f"{timestamp}_{args.op}.csv"

    # prepare CSV (without timestamp column)
    with open(report_file, 'w', newline='') as csvf:
        writer = csv.writer(csvf)
        writer.writerow([
            'run',
            'threads',
            'operations',
            'elapsed_ms',
            'ops_per_sec',
            'avg_time_ms',
        ])

        for i in range(1, args.runs + 1):
            cmd = (
                f"{hadoop_home}/bin/hadoop "
                "org.apache.hadoop.hdfs.server.namenode.NNThroughputBenchmark "
                f"-fs {args.fs} "
                f"-op {args.op} "
                f"-threads {args.threads} "
                f"-files {args.files}"
            )
            print(f"[{i}/{args.runs}] Running: {cmd}")
            stats = run_benchmark(cmd)

            writer.writerow([
                i,
                args.threads,
                stats['operations'],
                stats['elapsed_ms'],
                stats['ops_per_sec'],
                stats['avg_time_ms'],
            ])

    print(f"Done. Results written to {report_file}")

if __name__ == '__main__':
    main()
