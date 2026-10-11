# High-Throughput Rate Limiter & Concurrency Control Lab

> A benchmarkable, production-grade API gateway core exploring **in-memory algorithm variants**, **atomic distributed coordination via Redis Lua**, **process-level striped state (sharding)** to eliminate lock contention, and **fail-fast load shedding**.

---

## Summary

```
                       ┌────────────────────────────────────────────────────────┐
                       │                     Incoming HTTP                      │
                       └──────────────────────────┬─────────────────────────────┘
                                                  │
                 ┌────────────────────────────────┴────────────────────────────────┐
                 ▼                                                                 ▼
      [ Load Shedder (503) ]                                             [ Rate Limiter (429) ]
   Bounded Concurrency Semaphore                                      Policy Enforcement Layer
   Rejects excess in-flight work                                       Guards throughput over time
                 │                                                                 │
                 ├──────────────────────────────┬──────────────────────────────────┤
                 ▼                              ▼                                  ▼
      ┌──────────────────────┐      ┌──────────────────────┐      ┌──────────────────────────────┐
      │   In-Memory Sharded  │      │     Local Single     │      │   Distributed Shared Redis   │
      │    Striped State     │      │     Global Lock      │      │       Atomic Lua Script      │
      │  FNV-1a Hash Shards  │      │  High Lock Wait Lat. │      │  Multi-Instance Consistency  │
      └──────────────────────┘      └──────────────────────┘      └──────────────────────────────┘
```

* **4 In-Memory Algorithms**: Fixed Window, Token Bucket, Sliding Window Log, and Leaky Bucket (GCRA).
* **Distributed Atomic Enforcement**: Distributed Token Bucket, Fixed Window, and Sliding Window backed by atomic Redis Lua scripts to eliminate race conditions between decoupled instances.
* **Lock Contention Mitigation**: In-memory state striped across $N$ shards via FNV-1a hashing to prevent CPU core stalls and lock queue thrashing.
* **Fail-Fast Load Shedding**: Buffered channel-based concurrency semaphore immediately returning `503 Service Unavailable` on resource saturation, preventing unbounded tail-latency queues.

---

## System Architecture & Design Concepts

### 1. The Distinction: Rate Limiting vs. Load Shedding

| Dimension | Rate Limiter (`429 Too Many Requests`) | Load Shedder (`503 Service Unavailable`) |
| :--- | :--- | :--- |
| **Protected Scope** | Downstream dependencies & per-tenant fairness | Service process survivability & compute resources |
| **Metric** | Work **over time** ($X$ req / $Y$ window) | Concurrent work **in-flight right now** ($M$ active requests) |
| **Failure Mode** | Tenant exceeds fair quota; others continue | System hits saturation; drops excess requests instantly |
| **Response** | `429` with `Retry-After: <delta>` | `503` with fast failure to prevent cascading timeouts |

---

### 2. Algorithm Comparison & Complexity Trade-Offs

| Algorithm | State Space | Time Complexity | Burst Handling | Edge-Case Weakness |
| :--- | :--- | :--- | :--- | :--- |
| **Fixed Window** | $\mathcal{O}(1)$ | $\mathcal{O}(1)$ | No | **Boundary Spike**: Allows $2\times$ quota across the window boundary. |
| **Token Bucket** | $\mathcal{O}(1)$ | $\mathcal{O}(1)$ | Yes (up to capacity) | Burst parameters must be carefully calibrated to downstream limits. |
| **Sliding Window Log** | $\mathcal{O}(N)$ ($N$ = reqs in window) | $\mathcal{O}(\log N)$ or $\mathcal{O}(N)$ | Smooth | **Memory Consumption**: High throughput per key scales Redis/RAM footprint linearly. |
| **Leaky Bucket (GCRA)** | $\mathcal{O}(1)$ | $\mathcal{O}(1)$ | Strict zero burst | Paces traffic strictly; forces even spacing regardless of client demand. |

---

### 3. Distributed State Coordination (Redis + Atomic Lua)

When scaling across horizontal service replicas, in-memory limiters fail because state is uncoordinated. Naive Redis implementations (`GET` then `SET`) introduce critical **check-then-act** race conditions under concurrent network I/O.

This lab implements all shared limiters via **atomic Lua scripts** evaluated directly inside the Redis single-threaded execution model:

```
Instance A ───┐
              ├─► EVALSHA (Atomic Lua Script) ──► Single-Threaded Redis Engine ──► [Allow / Deny]
Instance B ───┘
```

* **Token Bucket Script**: Computes fractional token replenishment since `last_seen` timestamp on-the-fly and atomically decrements without client-side locking.
* **Sliding Window Script**: Employs a Redis Sorted Set (`ZSET`) where score = timestamp, purges scores older than $now - window$, and inspects `ZCARD` within one atomic operation.

---

### 4. Process-Level Striped State (Local Sharding)

In a high-throughput single-process Go environment, guarding an entire `map[string]State` with a single `sync.Mutex` causes serious thread contention across independent CPU cores.

#### Striped State Architecture:
Instead of 1 lock for all keys, the map is sharded into $N$ isolated buckets ($N$ = power of 2):

$$\text{Shard Index} = \text{FNV-1a}(Key) \pmod N$$

```
Key "user_abc" ──► FNV-1a % 16 ──► Shard 3  [Mutex 3 + Isolated Map]  (Unrelated keys run in parallel)
Key "user_xyz" ──► FNV-1a % 16 ──► Shard 11 [Mutex 11 + Isolated Map]
```

> **Engineering Note**: This is **process-level lock striping**, not distributed sharding. It reduces CPU lock wait time on a single node; it does not replace Redis across multiple instances.

---

## Benchmark Results

Benchmarks run on Apple M-series hardware using Go's parallel benchmarking harness (`b.RunParallel`):

```text
go test -bench=. -benchmem ./internal/shards/...
```

```
BenchmarkUnstriped-8    3124800    381.2 ns/op    0 B/op    0 allocs/op
BenchmarkStriped-8     14890214     78.4 ns/op    0 B/op    0 allocs/op
```

* **~4.8x Throughput Improvement**: Striped state across 16 shards reduces lock wait latency by **79.4%** under high concurrent load across unrelated client keys.

---

## Quickstart & Demo

### 1. Prerequisites
* Go 1.22+
* Docker (for local Redis)

### 2. Start Shared Redis
```bash
docker run --name rate-limiter-redis -p 6379:6379 -d redis:7-alpine
```

### 3. Run the Automated Demo
The repository includes an end-to-end verification script demonstrating multi-instance coordination and load shedding:

```bash
./demo.sh
```

```text
=== DEMO 1: Distributed Rate Limit Sharing across Instances ===
Sending 3 requests to Instance A on :8081...
Inst A Req 1: 200
Inst A Req 2: 200
Inst A Req 3: 200

Sending 2 requests to Instance B on :8082...
Inst B Req 1: 200
Inst B Req 2: 200

Sending 6th request to Instance A (Exceeds shared Redis limit of 5)...
Inst A Req 6: 429

=== DEMO 2: Fast 503 Load Shedding Under Concurrency ===
Sending 5 concurrent requests against MAX_CONCURRENT=2...
Work Req 1: 503
Work Req 2: 503
Work Req 3: 503
Work Req 4: 200
Work Req 5: 200
==> Demo completed successfully.
```

---

## Configuration Reference

All behavior is configured via environment variables:

| Variable | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `PORT` | `string` | `8080` | HTTP listener port. |
| `ALGORITHM` | `string` | `fixed_window` | Options: `fixed_window`, `token_bucket`, `sliding_window`, `leaky_bucket`. |
| `RATE_LIMIT` | `int` | `10` | Request budget permitted per window. |
| `WINDOW` | `duration` | `1m` | Time duration for rate calculation window (e.g., `10s`, `1m`). |
| `BURST` | `int` | `5` | Maximum token bucket burst capacity. |
| `STRIPED` | `bool` | `false` | Enables local process lock striping. |
| `SHARDS` | `int` | `16` | Number of shards for lock striping. |
| `MAX_CONCURRENT` | `int` | `50` | Concurrency ceiling before triggering load shedding (`503`). |
| `REDIS_ENABLED` | `bool` | `false` | Enables distributed Redis-backed atomic limiter. |
| `REDIS_ADDR` | `string` | `localhost:6379` | Network address of Redis instance. |

---

## 📂 Repository Layout

```text
.
├── cmd/
│   └── server/
│       └── main.go                  # Server lifecycle, dependency injection & routing
├── internal/
│   ├── config/                      # Environment variable parsing and validation
│   ├── limiter/                     # Rate limiting core
│   │   ├── factory.go               # Strategy pattern factory
│   │   ├── fixed_window.go          # In-memory & Redis atomic fixed window
│   │   ├── token_bucket.go          # In-memory & Redis atomic token bucket
│   │   ├── sliding_window.go        # In-memory slice & Redis ZSET sliding log
│   │   └── leaky_bucket.go          # In-memory & Redis atomic GCRA
│   ├── middleware/
│   │   ├── ratelimit.go             # Per-key identity extraction & limiter interceptor
│   │   └── loadshed.go              # Semaphore-bounded concurrency guard
│   └── shards/
│       ├── striped.go               # FNV-1a sharding decorator
│       └── striped_test.go          # Contention benchmarks and unit tests
├── scripts/
│   └── demo.sh                      # Reproducible multi-instance demo script
├── tests/
│   └── integration/
│       └── redis_test.go            # Multi-instance distributed integration test
├── Makefile
└── README.md
```

---

## ⚠️ Known Limitations & Follow-ups

* **Redis Clock Skew**: The sliding window log relies on host timestamps pushed into Lua. In real multi-host deployments, using Redis `TIME` prevents discrepancies caused by NTP drift across app nodes.
* **Sliding Window Memory**: At scale (millions of users), exact sliding window logs using Redis Sorted Sets require significant memory. In production, this can be substituted with a **two-counter sliding window approximation** ($\text{count} = \text{prev} \times (1 - \text{ratio}) + \text{curr}$).
* **Dynamic Load Shedding**: The current load shedder uses a static concurrency limit. A production evolution would incorporate dynamic load shedding based on CPU/memory telemetry (CoDel or queue delay tracking).