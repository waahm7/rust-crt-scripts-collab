#!/usr/bin/env python3
import argparse
from pathlib import Path
import re
import subprocess
import statistics

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
    help='HTTP clients to benchmark')
parser.add_argument(
    '--min-concurrency', type=int, default=1,
    help='Minimum concurrency to test')
parser.add_argument(
    '--max-concurrency', type=int, default=1024,
    help='Maximum concurrency to test')
parser.add_argument(
    '--samples', type=int, default=10,
    help='Num samples per run')
parser.add_argument(
    '--csv', default='out.csv',
    help='output results to this file')
parser.add_argument(
    '--url', default='s3://graebm-s3-benchmarks/download/8MiB-1x/1',
    help='URL of object to be downloaded. s3:// URLS will get presigned.')


def run(cmd_args: list[str], check=True) -> subprocess.CompletedProcess:
    """Run a subprocess"""
    print(f'{Path.cwd()}> {subprocess.list2cmdline(cmd_args)}', flush=True)

    # Subprocess doesn't have built-in support for capturing output
    # AND printing while it comes in, so we have to do it ourselves.
    # We're combining stderr with stdout, for simplicity.
    with subprocess.Popen(
        cmd_args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,  # line-buffered
    ) as p:
        lines = []
        assert p.stdout is not None  # satisfy type checker
        for line in p.stdout:
            lines.append(line)
            print(line, end='', flush=True)

        p.wait()  # ensure process is 100% finished

        completed = subprocess.CompletedProcess(
            args=cmd_args,
            returncode=p.returncode,
            stdout="".join(lines),
        )

        if completed.returncode != 0:
            if check:
                exit(f"FAILED running: {subprocess.list2cmdline(cmd_args)}")
            else:
                print(f"EXIT-CODE={completed.returncode} BUT CONTINUE...")

        return completed


def concurrency_range(min_val, max_val):
    all = [min_val]

    # keep doubling the value (but if it's too big, add a step in-between too)
    while all[-1] <= max_val:
        last = all[-1]
        double = last * 2
        step_size = double - last
        if step_size >= 50:
            all.append(int(last + step_size / 2))
        all.append(int(double))

    # ensure we end with max_val
    while all[-1] > max_val:
        all.pop()
    if all[-1] != max_val:
        all.append(max_val)

    return all


def median_throughput(stdout):
    samples = []
    for line in stdout.splitlines():
        m = re.match(r'Secs:\d+ Gb/s:(\d+\.\d+)', line)
        if m:
            samples.append(float(m.group(1)))

    # if this run completely failed, just report 0 and move on
    if not samples:
        return 0.0

    return statistics.median(samples)


if __name__ == '__main__':
    args = parser.parse_args()

    url = args.url
    if url.startswith('s3://'):
        presign_run = run(['aws', 's3', 'presign', url,
                          '--expires-in', str(12 * 60 * 60)])
        url = presign_run.stdout.strip()

    with open(args.csv, 'w') as csv:
        csv.write("CLIENT,CONCURRENCY,THROUGHPUT\n")

        for client in args.clients:
            for concurrency in concurrency_range(args.min_concurrency, args.max_concurrency):
                duration_secs = args.samples

                result = run([CLIENTS[client]['bin'], str(concurrency), str(duration_secs), url],
                             # ignore errors, keep going...
                             check=False)
                throughput = median_throughput(result.stdout)

                csv.write(f"{client},{concurrency},{throughput}\n")
                csv.flush()

    print("DONE!")
