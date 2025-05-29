# Distributed Key-Value Store

A lightweight, self‑contained, **Go** implementation of a distributed key‑value store that can run in two modes:

* **Standalone** — single‑node, embedded WAL + embedded LSM engine (RoseDB).
* **Cluster** — sharded across writers, each with synchronous read‑replicas, automatic resharding and fail‑over coordinated through ZooKeeper.

The project is designed for experimentation around consensus‑less replication, slot‑based sharding and high availability.

## Disclaimer

> **Not production ready.**  The codebase is a research project. Use at your own risk.

## Quickstart (Simulate cluster & test via CLI)

**1. Prerequisites**
* Go >= 1.22
* ZooKeeper running — easiest is Docker:
    ```bash
    docker-compose up -d
    ```

**2. Start simulated cluster**

This will start one writer instance, and one read‑replica, all running in the same process.
```bash
go run examples/cluster/main.go
```

**3. Build the CLI**
```bash
make build-cli
```

**4. Run CLI**
```bash
./bin/cli localhost:17000
```

## Design & Trade‑offs

### 1. Partitioning

* Uses hash slots as a partitioning scheme, similar to Redis.
* **1024 fixed slots** per key‑space (CRC‑32 % 1024) keeps hashing cheap while allowing dynamic range assignment.
* Writers own disjoint slot ranges; adding a new writer triggers *online resharding* that copies WAL segments for its range.
* Trade‑off: fixed slot count simplifies look‑ups but caps horizontal scalability.

### 2. Replication & Consistency

* **Synchronous quorum replication** (2‑phase *Prepare* / *Commit* over RPC) per write.
* Parameters: `replicationFactor` (N) & `writeQuorum` (Q). A write succeeds when **Q ≤ N** replicas ACK the commit.
* Consistency model: **Read‑your‑writes** on writers; **eventual** on replicas.
* Trade‑off: to avoid complexity it sacrifices linearizability during fail‑over.

### 3. Storage

* **WAL first, LSM later**: every mutation is encoded `Op|Key|Value` and appended to a per‑node WAL before being applied to RoseDB.
* Abstracted `FileSystem` interface lets you swap disk for in‑memory mocks in tests.
* Trade‑off: single WAL file per node limits parallelism; compaction logic delegated to RoseDB.

### 4. Coordination

* **ZooKeeper** stores cluster metadata (node IDs, slot ranges, status) and provides watch‑based notifications.
* Each node exposes an RPC server (`CurrentOffset`, `Prepare`, `Commit`, etc.).
* Trade‑off: external dependency simplifies service discovery but introduces operational overhead.

### 5. Fault Tolerance & Recovery

* Writers promote replicas using a best‑effort election if the writer goes down and quorum is still reachable.
* WAL offset checks + rollback paths guard against partial commits when a replica dies mid‑transaction.
* Trade‑off: no long‑running leader election; transient split‑brain possible under extreme network partitions.
