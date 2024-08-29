#!/usr/bin/env python3
import argparse
from pathlib import Path
import psutil
import re
import subprocess
from statistics import median

REPO_DIR = Path(__file__).parent.parent
CLIENTS = {
    'crt-http': {
        'bin': REPO_DIR/'crt-http-benchmark/build/crt-http-benchmark'
    },
    'rust-hyper': {
        'bin': REPO_DIR/'hyper-benchmark/target/release/hyper-benchmark'
    },
}

parser = argparse.ArgumentParser("Gather data from HTTP benchmarks")
parser.add_argument(
    '--clients', nargs='+', choices=CLIENTS.keys(), default=CLIENTS.keys(),
    help='List of HTTP clients to test')
parser.add_argument(
    '--concurrencies', nargs="+", type=int,
    default=[1, 10, 50, 100, 150, 200, 225, 250, 275, 300, 500, 1000],
    help='List of concurrency numbers to test')
parser.add_argument(
    '--secs', type=int, default=20,
    help='Num seconds to run each test')
parser.add_argument(
    '--csv', default='out.csv',
    help='Output results to this file')
parser.add_argument(
    '--action', default='upload',
    help='upload or download workload')
parser.add_argument(
    '--url', default='s3://graebm-s3-benchmarks/download/8MiB-1x/1',
    help='URL of object to be downloaded. s3:// URLS will get presigned.')


def run(cmd_args: list[str], check=True):
    print(f'{Path.cwd()}> {subprocess.list2cmdline(cmd_args)}', flush=True)
    return subprocess.run(cmd_args, check=check, text=True, capture_output=True)


def run_benchmark(cmd_args: list[str]):
    print(f'{Path.cwd()}> {subprocess.list2cmdline(cmd_args)}', flush=True)

    # Benchmarks print this pattern to report throughput samples
    throughput_pattern = re.compile(r'Secs:\d+ Gb/s:(\d+\.\d+)')

    # We'll gather samples as the benchmark runs
    throughput_samples = []
    cpu_samples = []
    mem_samples = []

    # use psutil (instead of subprocess.Popen) so we can gather process stats
    with psutil.Popen(
        cmd_args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,  # line-buffered
    ) as p:
        assert p.stdout is not None  # satisfy type checker
        cpu_count = psutil.cpu_count()
        p.cpu_percent()  # initial call always returns 0

        for line in p.stdout:
            throughput_match = throughput_pattern.match(line)
            if throughput_match:
                # whenever benchmark prints the throughput line
                # gather CPU and Mem stats too
                throughput = float(throughput_match.group(1))
                with p.oneshot():
                    cpu_percent = p.cpu_percent()
                    if cpu_percent != 0:
                        cpu_percent /= cpu_count

                    mem_mebibytes = p.memory_info().rss
                    if mem_mebibytes != 0:
                        mem_mebibytes /= (1024 * 1024)

                throughput_samples.append(throughput)
                cpu_samples.append(cpu_percent)
                mem_samples.append(mem_mebibytes)
                print(
                    f"Secs:{len(throughput_samples)} Gb/s:{throughput:.6f} CPU%:{cpu_percent:.1f} RSS(MiB):{mem_mebibytes:.1f}", flush=True)

            else:
                print(line, end='', flush=True)

        p.wait()  # ensure process is 100% finished

        if p.returncode != 0:
            print(f"EXIT-CODE={p.returncode} BUT CONTINUE...")

    return (median(throughput_samples), median(cpu_samples), median(mem_samples))


if __name__ == '__main__':
    args = parser.parse_args()

    url = args.url
    if url.startswith('s3://'):
        presign_run = run(['aws', 's3', 'presign', url,
                          '--expires-in', str(12 * 60 * 60)])
        url = presign_run.stdout.strip()

    with open(args.csv, 'w') as csv:
        csv.write("CLIENT\tCONCURRENCY\tTHROUGHPUT(Gb/s)\tCPU(%)\tMEM(MiB)\n")

        for client in args.clients:
            for concurrency in args.concurrencies:
                throughput, cpu, mem = run_benchmark(
                    [CLIENTS[client]['bin'], str(concurrency), str(args.secs), str(args.action), url])
                csv.write(
                    f"{client}\t{concurrency}\t{throughput:.6f}\t{cpu:.1f}\t{mem:.1f}\n")
                csv.flush()

    print("DONE!")
