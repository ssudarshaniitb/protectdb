# ProtectDB: PostgreSQL-Based Deterministic Database Engine with Native Dynamic Merkle Data Integrity & Distributed Consensus

[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-12%2B-blue.svg)](https://www.postgresql.org/)
[![License](https://img.shields.io/badge/License-PostgreSQL-lightgrey.svg)](LICENSE)
[![Build Status](https://img.shields.io/badge/Build-Passing-brightgreen.svg)]()

**ProtectDB** is a deterministic concurrency control database system built into the core of PostgreSQL. It combines deterministic transaction execution based on the Aria protocol with native, dynamic Merkle tree indexing for high-performance cryptographic data integrity, state verification, fast fault-recovery, and distributed Raft/Kafka-backed consensus replication.

---

## 🔬 Testing & Benchmarking

### 1. Run PostgreSQL & Merkle Regression Tests

```bash
# Run full PostgreSQL regression test suite
make check

# Or target Merkle index regression specs
cd src/test/regress
./pg_regress merkle_basic merkle_verify merkle_proof
```

### 2. Run Merkle Recovery Benchmark Suite

```bash
# Execute recovery benchmark across corrupted dynamic replica
./.venv/bin/python3 scripts/benchmark/recovery/run_merkle_recovery_benchmark.py \
  --dsn "host=127.0.0.1 port=5432 dbname=postgres user=postgres" \
  --profile preflight
```

For complete benchmarking flags, profiles, and plot generation, refer to [`scripts/benchmark/recovery/README.md`](scripts/benchmark/recovery/README.md).

### 3. Distributed 4-Node Raft Cluster Benchmark

```bash
# Launch 4-node Raft-Kafka cluster benchmark
./scripts/distributed/run_4node_raft_cluster.sh --threads 8

# Verify replica state consistency & Merkle root alignment across all nodes
./scripts/distributed/test_merkle_consistency.sh
```

For automated executor worker sweeps across cluster nodes, refer to [`scripts/distributed/RUN_SWEEP_README.md`](scripts/distributed/RUN_SWEEP_README.md).

### 4. Multi-Mode Benchmark Sweep (PG vs BCDB Det vs BCDB Merkle vs 4-Node Cluster)

The orchestrator script [`scripts/distributed/run_all_modes_gateway_sweep.py`](scripts/distributed/run_all_modes_gateway_sweep.py) automates comparative performance sweeps across all 4 execution modes with physical machine separation (Gateway client at `10.129.27.111` and Database server at `10.129.148.247:5438`):

1. **`pg`**: Plain vanilla PostgreSQL baseline (non-deterministic, dbType=0, no Merkle index).
2. **`bcdb_det`**: BCDB deterministic concurrency control without Merkle index (dbType=1, `enable_merkle_index=off`).
3. **`bcdb_merkle`**: BCDB deterministic concurrency control with dynamic Merkle tree indexing (dbType=1, `enable_merkle_index=on`).
4. **`cluster`**: Full 4-node Raft + Kafka distributed cluster execution.

#### Executing the Sweep

To run the complete 4-mode benchmark sweep across low-skew and high-skew YCSB workloads with worker scaling from 1 to 24:

```bash
python3 scripts/distributed/run_all_modes_gateway_sweep.py \
  --modes pg,bcdb_det,bcdb_merkle,cluster \
  --workers 1,2,4,8,12,16,20,24 \
  --workloads "scripts/ycsbtx-skew-01-24k-pt-intkey-sid-clean-20k.txt,scripts/ycsb-skew0-99-tx-20k-point-safedb-intkey-insert12k-uniq.txt" \
  --run-cluster
```

#### Benchmark Results Summary

The sweep validates strict replica consistency (`divergence_count=0`, `permanent_failures=0`, Merkle PASS=1) and generates comparative throughput metrics and scaling plots (live console output captured in [`out_4modes_live.txt`](out_4modes_live.txt)):

| Workload | Workers | PG TPS | BCDB Det TPS | BCDB Merkle TPS | Cluster TPS | Merkle vs Cluster (%) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **Low-Skew (0.01)** | 1 | 3,602.4 | 3,015.8 | 2,721.6 | 2,717.9 | +0.14% |
| Low-Skew (0.01) | 2 | 5,657.2 | 4,090.8 | 3,635.8 | 3,661.7 | -0.71% |
| Low-Skew (0.01) | 4 | 10,274.3 | 5,647.7 | 5,129.2 | 5,029.9 | +1.97% |
| Low-Skew (0.01) | 8 | 16,410.2 | 8,689.8 | 7,878.7 | 7,762.5 | +1.50% |
| Low-Skew (0.01) | 12 | 17,170.8 | 10,754.8 | 9,677.8 | 9,378.3 | +3.19% |
| Low-Skew (0.01) | 16 | 16,923.9 | 11,843.7 | 10,180.2 | 9,724.8 | +4.68% |
| Low-Skew (0.01) | 20 | 17,170.8 | 12,249.8 | 10,534.0 | 9,937.4 | +6.00% |
| Low-Skew (0.01) | 24 | 16,923.9 | 12,072.4 | 10,495.3 | 9,369.6 | +12.01% |
| **High-Skew (0.99)** | 1 | 6,862.8 | 5,139.8 | 4,722.1 | 4,688.7 | +0.71% |
| High-Skew (0.99) | 2 | 11,543.6 | 7,336.6 | 6,703.6 | 6,629.9 | +1.11% |
| High-Skew (0.99) | 4 | 16,371.1 | 9,514.4 | 8,758.8 | 8,028.6 | +9.09% |
| High-Skew (0.99) | 8 | 17,252.3 | 12,045.2 | 10,893.8 | 11,270.9 | -3.35% |
| High-Skew (0.99) | 12 | 17,698.9 | 14,486.6 | 13,107.3 | 12,917.5 | +1.47% |
| High-Skew (0.99) | 16 | 17,899.7 | 15,516.6 | 14,156.7 | 13,983.0 | +1.24% |
| High-Skew (0.99) | 20 | 17,714.2 | 16,463.1 | 15,172.3 | 14,778.8 | +2.66% |
| High-Skew (0.99) | 24 | 17,517.5 | 17,023.2 | 15,706.7 | 15,423.3 | +1.84% |

Outputs, raw CSV metrics, and generated comparison plots (`final_tps_all_modes_comparison.png`) are saved under `scripts/bench_full_results/all_modes_gateway_sweep_<timestamp>/`.

### 5. Launch Live Dynamic Merkle Inspector

```bash
# Run inspector against local active database
MERKLE_VIZ_CONNINFO='host=127.0.0.1 port=5432 dbname=postgres user=postgres' \
  ./.venv/bin/python3 dynamic_merkle_visualizer/app.py
```
Open `http://127.0.0.1:8787` in your browser to inspect live physical page layouts, partition roots, and Copy-on-Write record evolution.

---


