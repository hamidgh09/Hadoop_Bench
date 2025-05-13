#!/usr/bin/env python3
import os
import subprocess
import yaml
import itertools
import csv
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# 1) Load config
def load_config(path="config.yaml"):
    with open(path) as f:
        return yaml.safe_load(f)

def run_benchmark(fs, op, threads, files):
    cmd = (
        f"$HADOOP_HOME/bin/hadoop "
        "org.apache.hadoop.hdfs.server.namenode.NNThroughputBenchmark "
        f"-fs {fs} -op {op} -threads {threads} -files {files}"
    )
    print(cmd)
    proc = subprocess.run(cmd, shell=True,
                          stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT,
                          universal_newlines=True)
    out = proc.stdout

    # parse stats
    import re
    patterns = {
        'operations': re.compile(r"# operations:\s*(\d+)"),
        'elapsed_ms': re.compile(r"Elapsed Time:\s*(\d+)"),
        'ops_per_sec': re.compile(r"Ops per sec:\s*([\d\.]+)"),
        'avg_time_ms': re.compile(r"Average Time:\s*(\d+)")
    }
    return {
        k: (pat.search(out).group(1) if pat.search(out) else None)
        for k, pat in patterns.items()
    }


def main():
    # Load flat YAML config
    cfg = load_config()
    fs           = cfg['fs']
    runs         = cfg['runs']
    operations   = cfg['op']
    threads_list = cfg['threads']
    files_list   = cfg['files']

    # Prepare output directory and CSV
    timestamp  = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_dir = 'results'
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f'results_{timestamp}.csv')

    with open(output_file, 'w', newline='') as csvf:
        writer = csv.writer(csvf)
        writer.writerow([
            'run_id',
            'operation',
            'threads',
            'files',
            'operations',
            'ops_per_sec',
        ])

        # Sweep over all combinations of op, thread count, and file count
        combos = list(itertools.product(operations, threads_list, files_list))
        run_id = 1
        for op, threads, files in combos:
            for _ in range(runs):
                stats = run_benchmark(fs, op, threads, files)
                writer.writerow([
                    run_id,
                    op,
                    threads,
                    files,
                    stats['operations'],
                    stats['ops_per_sec'],
                ])
                print(f"Run {run_id}: op={op}, threads={threads}, files={files} → {stats}")
                run_id += 1

    print(f"Benchmarking complete. Results written to {output_file}")

if __name__ == '__main__':
    main()