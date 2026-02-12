import json
import re
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np
import csv
import argparse
import os

parser = argparse.ArgumentParser(description='Auto-discover all result patterns, compare systems, and plot bar graphs')
parser.add_argument("-r", "--results_dir", type=str, default="results",
                    help="Root results directory (default: setup/results)")
parser.add_argument("-o", "--output_dir", type=str, default="plots",
                    help="Root output directory for plots (default: setup/plots)")
args = parser.parse_args()

# ---- Helper functions (from klatency_all.py) ----

def calculate_sorensen_similarity(integer_sets):
    nsets = len(integer_sets)
    keys_union = set().union(*integer_sets)
    total_size = sum(len(s) for s in integer_sets)
    nunique = len(keys_union)
    if nsets == 1:
        scale = 1
    else:
        scale = nsets / (nsets - 1)
    if (nunique / total_size) == 1:
        sorensen_similarity = 1
    else:
        sorensen_similarity = scale * (1 - (nunique / total_size))
    return sorensen_similarity

def extract_access_rate_from_filename(filename):
    access_rate_details = {
        'access_rate': [], 'local': [], 'remote': [], 'performance': [],
        'dup_allowed': [], 'curr_dup': [], 'system_dup': []
    }
    file_data = open(filename, 'r').read()
    for line in file_data.splitlines():
        details = line.split(";")
        if len(details) > 2:
            access_rate_details['access_rate'].append(int(details[0]))
            access_rate_details['local'].append(int(details[1]))
            access_rate_details['remote'].append(int(details[2]))
            access_rate_details['performance'].append(int(details[3]))
            access_rate_details['dup_allowed'].append(int(details[4]))
            access_rate_details['curr_dup'].append(int(details[5]))
            access_rate_details['system_dup'].append(int(details[6]))
    return access_rate_details

def read_integers(file_path):
    with open(file_path, 'r') as file:
        integers = set(map(int, file.read().splitlines()))
    return integers

def average(lst):
    return sum(lst) / len(lst) if lst else 0

def write_to_csv(sorted_results, filename):
    headers = ['System', 'CacheSize', 'Throughput', 'Avg Latency', 'Latency50', 'Latency99',
               'MissRate', 'RemoteHitRate', 'LocalHitRate', 'Disk_access', 'Remote_disk',
               'DataCoverage', 'Similarity', 'SorensenSimilarity', 'AccessRate']
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        for result in sorted_results:
            writer.writerow([
                result['system_char'],
                result['details']['cache_size'],
                result['throughput'],
                result['average_latency_us'],
                result['p50_latency'],
                result['p99_latency'],
                result['miss_rate'],
                result['remote_hit_rate'],
                result['local_hit_rate'],
                result['disk_accesses'],
                result['remote_disk_accesses'],
                result['data_coverage'],
                result['similarity'],
                result['sorensen_similarity'],
                result['details']['access_rate']
            ])

# ---- Core: process a single experiment subdirectory ----

def process_subdirectory(subdirectory):
    system_type = subdirectory.name.split("::")[0]
    parts = subdirectory.name.split("::")[1].split('_')
    system_char = parts[0]

    count = 0
    for part in parts:
        if part.endswith('0'):
            break
        count += 1

    details = {
        'distribution': parts[1],
        'cache_size': int(int(parts[count]) / 1000),
        'num_servers': int(parts[count + 1][2:]),
        'num_clients': int(parts[count + 2][2:]),
        'num_clients_per_thread': int(parts[count + 3][4:]),
        'num_threads': int(parts[count + 4][2:]),
        'access_rate': int(parts[-1][4:])
    }

    num_total_nodes = details['num_clients'] + details['num_servers']

    throughput = 0
    average_latency = []
    p50_latencies = []
    p99_latencies = []
    total_reads = 0
    misses = 0
    remote_hits = 0
    local_hits = 0
    freq_addition = 0
    disk_accesses = 0
    remote_disk_accesses = 0
    integer_sets = []
    access_rate_details = {}

    for i in range(num_total_nodes):
        metrics_file = subdirectory / f'latency_results_{i}.json'
        if metrics_file.exists():
            with open(metrics_file, 'r') as file:
                data = json.load(file)
                throughput += data["total"].get("total_throughput", 0)
                average_latency.append(data["total"].get("average_latency_us", 0))
                p99_latencies.append(data["total"].get("p99", 0))
                p50_latencies.append(data["total"].get("p50_latency_us", 0))
        cache_metrics_file = subdirectory / f'cache_metrics_{i + details["num_clients"]}.json'
        if cache_metrics_file.exists():
            with open(cache_metrics_file, 'r') as file:
                cache_data = json.load(file)
                total_reads += cache_data["total_reads"]
                misses += cache_data["cache_miss"]
                remote_hits += total_reads - misses - remote_disk_accesses
                local_hits += cache_data["cache_hit"]
                freq_addition += cache_data["cache_freq_addition"]
                disk_accesses += cache_data["local_disk_access"]
                remote_disk_accesses += cache_data["remote_disk_access"]
        cache_dump_file = subdirectory / f'cache_dump_{i + details["num_clients"]}.txt'
        if cache_dump_file.exists():
            integer_sets.append(read_integers(cache_dump_file))
        access_rate_file = subdirectory / f'access_rate_{i + details["num_clients"]}.txt'
        if access_rate_file.exists():
            access_rate_details[f'node_{i + details["num_clients"]}'] = extract_access_rate_from_filename(access_rate_file)

    average_latency_us = average(average_latency)
    p99_latency_us = average(p99_latencies)
    p50_latency_us = average(p50_latencies)

    if total_reads == 0:
        total_reads = -1
    miss_rate = misses / total_reads
    if misses == 0:
        misses = -1
    remote_hit_rate = (misses - disk_accesses - remote_disk_accesses) / misses
    local_hit_rate = local_hits / total_reads
    disk_accesses = disk_accesses / total_reads
    remote_disk_accesses = remote_disk_accesses / total_reads

    if len(integer_sets) == 0 or len(integer_sets[0]) == 0:
        data_coverage = -1
        similarity = -1
        sorensen_similarity = -1
    else:
        union_keys = set().union(*integer_sets)
        intersection_keys = set(integer_sets[0]).intersection(*integer_sets[1:])
        data_coverage = len(union_keys) / 100000
        similarity = len(intersection_keys) / len(integer_sets[0])
        sorensen_similarity = calculate_sorensen_similarity(integer_sets)

    return {
        "folder": subdirectory.name,
        "system_type": system_type,
        "system_char": system_char,
        "details": details,
        "throughput": throughput,
        "average_latency_us": average_latency_us,
        "p99_latency": p99_latency_us,
        "p50_latency": p50_latency_us,
        "miss_rate": miss_rate,
        "remote_hit_rate": remote_hit_rate,
        "local_hit_rate": local_hit_rate,
        "disk_accesses": disk_accesses,
        "remote_disk_accesses": remote_disk_accesses,
        "data_coverage": data_coverage,
        "similarity": similarity,
        "sorensen_similarity": sorensen_similarity,
        "freq_addition": freq_addition
    }

# ---- Collect all experiment dirs from a single pattern directory ----

def collect_results(pattern_dir):
    """Scan all subdirectories in pattern_dir and return list of result dicts."""
    results = []
    for subdirectory in pattern_dir.iterdir():
        if not subdirectory.is_dir() or '::' not in subdirectory.name:
            continue
        try:
            results.append(process_subdirectory(subdirectory))
        except Exception as e:
            print(f"  Warning: skipping {subdirectory.name}: {e}")
    return results

# ---- Dedup: pick best throughput per (system_char, cache_size) ----

def pick_best_per_cache_size(results_list, system_char):
    by_cache = {}
    for r in results_list:
        if r['system_char'] != system_char:
            continue
        cs = r['details']['cache_size']
        if cs not in by_cache or r['throughput'] > by_cache[cs]['throughput']:
            by_cache[cs] = r
    return list(by_cache.values())

# ---- Plotting helpers ----

BASELINE_CHARS = {'A', 'B'}
LDC_CHARS = {'C', 'D'}

SYSTEM_LABELS = {
    'A': 'Baseline',
    'B': 'Baseline (B)',
    'C': 'LDC',
    'D': 'Perfect Baseline (D)',
}

SYSTEM_COLORS = {
    'A': '#4878CF',
    'B': '#6ACC65',
    'C': '#D65F5F',
    'D': '#B47CC7',
}

def fmt_throughput(v):
    if v >= 1e6:
        return f'{v/1e6:.2f}M'
    elif v >= 1e3:
        return f'{v/1e3:.1f}K'
    return f'{v:.0f}'

def fmt_latency(v):
    return f'{v:.1f}'

def fmt_pct(v):
    return f'{v:.2%}'

def fmt_float(v):
    return f'{v:.3f}'

METRICS = [
    ('throughput',          'Throughput (ops/sec)',   'Throughput',          'throughput.png',          fmt_throughput),
    ('p50_latency',         'Latency (us)',      'Latency',         'latency.png',        fmt_latency),
    ('similarity',          'Similarity',            'Similarity',          'similarity.png',          fmt_float),
]

def plot_grouped_bars(systems_data, all_cache_sizes, pattern_name, output_dir):
    """
    systems_data: dict  {system_char: {cache_size: result_dict}}
    Plots one set of bar graphs (all metrics) for this pattern.
    """
    chars = sorted(systems_data.keys())
    n_systems = len(chars)
    if n_systems == 0 or not all_cache_sizes:
        return

    x = np.arange(len(all_cache_sizes))
    total_width = 0.7
    bar_width = total_width / n_systems

    for metric_key, ylabel, base_title, filename, fmt_func in METRICS:
        fig, ax = plt.subplots(figsize=(10, 6))

        for idx, char in enumerate(chars):
            data = systems_data[char]
            vals = [data[cs][metric_key] if cs in data else 0 for cs in all_cache_sizes]
            offset = (idx - (n_systems - 1) / 2) * bar_width
            bars = ax.bar(x + offset, vals, bar_width,
                          label=SYSTEM_LABELS.get(char, char),
                          color=SYSTEM_COLORS.get(char, '#999999'),
                          edgecolor='black', linewidth=0.5)
            for bar in bars:
                height = bar.get_height()
                if height != 0:
                    label = fmt_func(height) if fmt_func else f'{height:.1f}'
                    ax.annotate(label,
                                xy=(bar.get_x() + bar.get_width() / 2, height),
                                xytext=(0, 3), textcoords="offset points",
                                ha='center', va='bottom', fontsize=8)

        title = f'{base_title} — {pattern_name}'
        ax.set_xlabel('Cache Size (% of dataset)', fontsize=13)
        ax.set_ylabel(ylabel, fontsize=13)
        ax.set_title(title, fontsize=15)
        ax.set_xticks(x)
        ax.set_xticklabels([str(cs * 10) for cs in all_cache_sizes], fontsize=11)
        ax.legend(fontsize=12)
        ax.grid(axis='y', alpha=0.3)

        plt.tight_layout()
        filepath = os.path.join(output_dir, filename)
        plt.savefig(filepath, dpi=150)
        plt.close()

# ---- Auto-discover and process all patterns ----

root = Path(args.results_dir)
if not root.exists():
    print(f"Results directory not found: {root}")
    exit(1)

# Discover pattern directories (subdirs of root that contain :: experiment dirs)
pattern_dirs = sorted([
    d for d in root.iterdir()
    if d.is_dir() and any('::' in sub.name for sub in d.iterdir() if sub.is_dir())
])

if not pattern_dirs:
    print(f"No result patterns found under {root}/")
    exit(1)

print(f"Found {len(pattern_dirs)} pattern(s): {[d.name for d in pattern_dirs]}\n")

for pattern_dir in pattern_dirs:
    pattern_name = pattern_dir.name
    print(f"{'='*60}")
    print(f"Processing: {pattern_name}")
    print(f"{'='*60}")

    results = collect_results(pattern_dir)
    if not results:
        print(f"  No valid results, skipping.\n")
        continue

    # Discover which system chars are present
    all_chars = sorted(set(r['system_char'] for r in results))
    print(f"  Systems found: {all_chars}")

    # Dedup: pick best throughput per (system_char, cache_size)
    deduped = []
    for char in all_chars:
        deduped.extend(pick_best_per_cache_size(results, char))

    # Build per-system data keyed by cache size
    systems_data = {}
    for char in all_chars:
        systems_data[char] = {}
        for r in deduped:
            if r['system_char'] == char:
                systems_data[char][r['details']['cache_size']] = r

    all_cache_sizes = sorted(set(r['details']['cache_size'] for r in deduped))
    print(f"  Cache sizes: {all_cache_sizes}")

    # Output dir for this pattern
    pattern_output = os.path.join(args.output_dir, pattern_name)
    os.makedirs(pattern_output, exist_ok=True)

    # Write CSVs
    sorted_results = sorted(deduped, key=lambda x: (x['system_char'], x['details']['cache_size']))
    sorted_by_tp = sorted(deduped, key=lambda x: x['throughput'], reverse=True)
    write_to_csv(sorted_results, os.path.join(pattern_output, 'metrics_summary.csv'))
    write_to_csv(sorted_by_tp, os.path.join(pattern_output, 'metrics_throughput_sorted.csv'))

    # Print summary
    for result in sorted_by_tp:
        sys_label = SYSTEM_LABELS.get(result['system_char'], result['system_char'])
        print(f"  [{sys_label}] cache={result['details']['cache_size']}K  "
              f"throughput={result['throughput']:.0f}  "
              f"avg_lat={result['average_latency_us']:.1f}us  "
              f"p99={result['p99_latency']:.1f}us  "
              f"miss={result['miss_rate']:.2%}  "
              f"access_rate={result['details']['access_rate']}")

    # Plot all metrics
    plot_grouped_bars(systems_data, all_cache_sizes, pattern_name, pattern_output)
    print(f"  Plots saved to: {pattern_output}/\n")

print(f"Done. All outputs under: {args.output_dir}/")
