# ProtectDB: PostgreSQL-Based Deterministic Database Engine with Native Dynamic Merkle Data Integrity & Distributed Consensus

[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-12%2B-blue.svg)](https://www.postgresql.org/)
[![License](https://img.shields.io/badge/License-PostgreSQL-lightgrey.svg)](LICENSE)
[![Build Status](https://img.shields.io/badge/Build-Passing-brightgreen.svg)]()

**ProtectDB** is a deterministic concurrency control database system built into the core of PostgreSQL. It combines deterministic transaction execution based on the Aria protocol with native, dynamic Merkle tree indexing for high-performance cryptographic data integrity, state verification, fast fault-recovery, and distributed Raft/Kafka-backed consensus replication.

---

## 🌟 Key Architecture & Capabilities

### 1. Deterministic Concurrency Control Engine (`src/backend/bcdb/`)
- **Deterministic Transaction Execution**: Eliminates distributed lock management overhead by batching and executing transactions deterministically in parallel epoch phases.
- **PostgreSQL Kernel Integration**: Native execution within PostgreSQL's backend engine (`src/backend/bcdb/`), preserving ACID guarantees and standard SQL query interfaces.

### 2. Native Dynamic Merkle Indexing Engine (`src/backend/access/merkle/`)
- **Native Table Access Method**: Implemented as a full PostgreSQL access method (`USING merkle`), supporting both single-key and multi-key indexes.
- **Dynamic Tree Geometry**: Supports dynamic leaf node splitting and merging driven by configurable fanout, split thresholds, and merge thresholds (`fanout`, `split_threshold`, `merge_threshold`).
- **Synchronous Copy-on-Write (COW)**: Ensures atomic tree updates during concurrent transaction processing with zero reader locking.
- **Cryptographic Security**: Uses **BLAKE3 (256-bit)** fast parallel hashing for leaf and internal tree nodes.
- **Dynamic Depth Progression**: Automatically scales tree depth (e.g., Levels 4 to 7+) across 1M to 50M+ tuples while maintaining bounded leaf geometry and constant-factor recovery overhead.

### 3. Distributed Consensus Gateway & Server (`protectdb_modules/`)
- **C++ Cluster Server & Gateway**: Built on [NuRaft](NuRaft/) for Raft consensus and native `librdkafka` for distributed client request streaming.
- **High-Throughput Partitioning**: Supports multi-node transaction ordering, result routing by transaction ID (`tx_id`/`req_id`), and durable logging.

### 4. Merkle Recovery & Verification Pipeline (`scripts/benchmark/recovery/`)
- **O(log N) Corruption Localization**: Fast tree descent identifying corrupted regions without scanning unchanged data blocks.
- **Automated State Repair**: Selective DML candidate fetch and execution (`INSERT`, `UPDATE`, `DELETE`) restoring byte-for-byte state alignment.
- **Comprehensive Audit**: Full Merkle root and record verification returning `divergence_count=0` and Merkle PASS signatures.

### 5. Live Dynamic Merkle Visualizer (`dynamic_merkle_visualizer/`)
- Interactive web dashboard for real-time inspection of live PostgreSQL buffers, MVCC partition roots, retained physical records (v8 layout), and tuple-to-leaf mappings.

---

## 📁 Repository Structure

```text
ProtectDB/
├── src/
│   ├── backend/
│   │   ├── bcdb/          # Core  deterministic concurrency engine
│   │   └── access/merkle/ # Native Dynamic Merkle Tree access method
│   └── include/
│       ├── bcdb/          # Header files for BCDB executor
│       └── access/merkle.h# Headers for Merkle tree access method
├── protectdb_modules/             # C++ Raft consensus server and Kafka gateway
│   ├── src/               # Gateway/server source code
│   └── README.md          # Gateway & cluster documentation
├── NuRaft/                # NuRaft C++ Raft consensus library
├── dynamic_merkle_visualizer/ # Live web inspector for dynamic Merkle trees
├── Dynamic_merkle_docs/   # Architecture whitepapers & recovery benchmark analysis
│   └── RECOVERY_ARCHITECTURE_ANALYSIS.md # 1M–50M performance analysis report
├── scripts/
│   ├── distributed/       # Cluster setup, 4-node benchmark scripts, sweep harness
│   └── benchmark/recovery/# Dynamic Merkle recovery benchmark suite
├── README_MERKLE.md       # Comprehensive Merkle Tree implementation documentation
├── AGENTS.md              # Repository guidelines and benchmark rules
└── README.md              # Main project documentation (this file)
```

---

## 🛠️ Build and Installation

### Prerequisites

Ensure the following system dependencies are installed:

- **Linux Build Environment**: GCC/Clang (C11 and C++11 standard support), Make, CMake (>= 3.26), Flex, Bison, Readline, Zlib.
- `sudo apt install release is older then try-

wget https://github.com/Kitware/CMake/releases/download/v3.29.3/cmake-3.29.3-linux-x86_64.sh

sudo ./cmake-3.29.3-linux-x86_64.sh --prefix=/usr/local --skip-license`

- **Libraries**: `librdkafka-dev` (or `librdkafka-devel` on RHEL/CentOS).
- **Python**: Python 3.8+ with virtualenv support (`pip install pytest matplotlib pandas numpy`).

### 1a. Build & Install PostgreSQL / ProtectDB Kernel

```bash
# Configure PostgreSQL/BCDB installation directory
./configure --prefix=/work/ProtectDB/install

# Build and install PostgreSQL backend
make -j$(nproc)
make install
```

### 1b. Build & Install Raft

```bash
cd NuRaft; ./prepare.sh ; mkdir build ;cd build;
```

### 2. Build C++ Cluster Gateway & Server Binaries

```bash
# Configure C++ CMake project
cmake -S protectdb_modules -B protectdb_modules/build -DCMAKE_BUILD_TYPE=Release

# Build gateway and server binaries
cmake --build protectdb_modules/build --target  protectdb_raft_db_coordinator protectdb_queryClient_resultAggregator -j$(nproc)
```

Binaries will be generated at:
- `protectdb_modules/build/bin/protectdb_raft_db_coordinator`
- `protectdb_modules/build/bin/protectdb_queryClient_resultAggregator`

---

## 🚀 Usage & Quick Start

### 1. Running Single-Node Workload

```bash
# 1. Initialize database cluster
/work/ARIABC/install/bin/initdb -D /tmp/ycsb_db

# 2. Launch database engine
/work/ARIABC/install/bin/postgres -D /tmp/ycsb_db &

# 3. Create database and set up tables
/work/ARIABC/install/bin/createdb ycsb
/work/ARIABC/install/bin/psql -d ycsb < src/benchmark/samples/ycsb_setup

# 4. Run sample workload block
/work/ARIABC/install/bin/psql -d ycsb < src/benchmark/samples/ycsb_tx_blocks
```

### 2. Creating a Dynamic Merkle Index

```sql
-- Create a Dynamic Merkle index with custom fanout and split/merge thresholds
CREATE INDEX usertable_dynamic_merkle_idx ON usertable
USING merkle (ycsb_key)
WITH (
    fanout = 32,
    split_threshold = 1024,
    merge_threshold = 256,
    dynamic = true
);

-- Verify Merkle tree integrity
SELECT merkle_verify('usertable');

-- Fetch Merkle tree root hash
SELECT merkle_root_hash('usertable');

-- Fetch node details
SELECT * FROM merkle_node_hash('usertable');
```


---

## 📚 Documentation Index

- **[Merkle Tree Implementation Details](README_MERKLE.md)**: In-depth technical architecture of the dynamic Merkle tree access method, BLAKE3 hashing, and SQL functions.
- **[Dynamic Merkle Recovery Analysis](Dynamic_merkle_docs/RECOVERY_ARCHITECTURE_ANALYSIS.md)**: Performance analysis report and empirical scaling results (1M–50M rows).
- **[C++ Cluster & Gateway Architecture](protectdb_modules/README.md)**: NuRaft and Kafka message broker integration details.
- **[Distributed Sweep Harness Guide](scripts/distributed/RUN_SWEEP_README.md)**: Instructions for running automated cluster benchmark sweeps.
- **[Repository Guidelines](AGENTS.md)**: Guidelines for contributing code, benchmark standards, and pull requests.

