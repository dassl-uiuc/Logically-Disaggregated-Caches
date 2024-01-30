# Artifact Evaluation: A Logically Disaggregated Cache for Replicated Storage Systems

This repository contains the artifact for the paper **"A Logically Disaggregated Cache for Replicated Storage Systems"** (Paper #563). The artifact includes the implementation of LDC in TWIG-KV, an eventually-consistent key-value store with primary-backup replication, along with scripts to run the system.


## Overview

LDC (Logically Disaggregated Cache) is a new architecture for managing embedded caches in replicated storage systems. Instead of each replica managing its cache in a silo, LDC disaggregates the embedded caches to form a single, unified, logical cache. LDC reduces cache redundancy from both reads (via one-sided RDMA to access remote caches) and writes (via selective quick demotions using a tiny queue). An online cost-benefit analyzer (CBA) balances cache coverage and redundancy.


## Prerequisites

### Hardware Requirements

- **CloudLab cluster** with **7 nodes** of type **xl170**
  - 3 nodes for servers (replicas)
  - 3 nodes for clients
  - 1 node for the coordinator (one of the server nodes can act as the coordinator if needed)
  - Each node: Intel 10-Core E5-2640v4 CPU, 64GB DRAM, 25Gb Mellanox ConnectX-4 NIC, 480GB SATA SSD

### Software Requirements

- Ubuntu 22.04 LTS (UBUNTU22-64-STD image on CloudLab)
- SSH key access configured on all nodes

> **Note for artifact evaluators**: The CloudLab environment is already pre-configured. You can skip directly to [Kick the Tires](#kick-the-tires-artifacts-functional).

## Build Source Code

### Step 1: Configure Username

All scripts share a single configuration file. Edit `setup/env.sh` and set your CloudLab username:

```bash
# setup/env.sh
USERNAME_RAW="your_cloudlab_username"
```

### Step 2: Configure CloudLab Cluster

Copy your CloudLab manifest XML (found under "Manifest" when viewing the experiment) to the config directory:

```bash
cp your_cloudlab_manifest.xml config/cloudlab_machines.xml
```

### Step 3: Initialize and Compile

```bash
cd setup/

# Install dependencies and set up all nodes (runs on all cluster nodes via SSH)
./init.sh

# IMPORTANT: Reboot all hosts in CloudLab before continuing

# Compile and deploy to all nodes
./init_compile.sh
```

This will:
- Install all dependencies (Machnet, RDMA libraries, Cap'n Proto, etc.) on every node
- Compile the TWIG-KV binary and deploy it to `/mnt/sda4/LDC/build` on all nodes

### Step 4: Generate YCSB Workloads

```bash
./create_ycsb_workload.sh
```

This generates YCSB traces for uniform, hotspot (80/20), and zipfian (0.99) distributions in `/mydata/ycsb_traces/` and `/mydata/ycsb/`.

## Kick the Tires (Artifacts Functional)

### Running Experiments

we run the following experiments:
 Cache Size: 0.10 and 0.334
 Workload: Uniform and Zipfian 0.99
 System: Baseline and LDC

To run both the baseline and LDC systems, run the following script:

```bash
cd setup/
./run_all.sh
```

To run only the baseline system, run the following script:

```bash
cd setup/
./run_baseline.sh
```

To run only the LDC system, run the following script:

```bash
cd setup/
./run_LDC.sh
```

To select the type of workload to run, go into the script of the system you want to run and at the end of the script, you will see the following:

```bash
CACHE_SIZE=(0.10 0.334)
uniform
zipfian_0.99
```
Change the CACHE_SIZE to the cache size you want to run it in the fraction of the dataset.
To select the pattern of the workload, comment out the other workload patterns and uncomment the workload pattern you want to run. By default, both uniform and zipfian_0.99 are run.


Results are stored under `setup/results/` organized by workload type (e.g., `results/uniform/`, `results/zipfian_0.99/`).

### Analyzing Results

After experiments complete, use the analysis scripts to generate plots:

```bash
cd setup/
python klatency_all.py -d zipfian
```