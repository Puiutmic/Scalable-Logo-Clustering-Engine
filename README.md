# Scalable Logo Clustering Engine

[![C++ CI](https://github.com/PetcuDavid/Scalable-Logo-Clustering-Engine/actions/workflows/cpp-ci.yml/badge.svg?branch=main)](https://github.com/PetcuDavid/Scalable-Logo-Clustering-Engine/actions/workflows/cpp-ci.yml)
**C++20 · Python · asyncio · CMake · GoogleTest · GitHub Actions · Docker**

A performance-focused pipeline for extracting visual brand assets from websites and clustering domains by logo similarity.

The system uses **Python for asynchronous web extraction and perceptual hashing**, then exports 64-bit hashes to a **C++20 clustering core** built around Hamming distance and Disjoint Set Union (Union-Find).

The C++ implementation preserves the behavior of the original Python clustering baseline while significantly reducing CPU-bound clustering time. The repository also includes deterministic benchmarks, automated cross-language correctness verification, multithreaded similarity evaluation, unit tests, cross-platform CI, and sanitizer checks.

---

## Results at a Glance

### Real web extraction sample

| Metric | Result |
|---|---:|
| Domains attempted | 300 |
| Logos extracted and hashed | 188 |
| Extraction success rate | 62.67% |
| Pairwise comparisons | 17,578 |
| Successful DSU unions | 44 |
| Final clusters | 144 |

The Python and C++ implementations produced **exactly the same cluster memberships** on this dataset.

```text
[PASS] Outputs are logically identical.
Every Python cluster has exactly the same members as its C++ counterpart.
```

### Performance benchmark

The equivalent C++20 implementation achieved approximately **67x–92x speedup** over the Python clustering baseline across the tested synthetic workloads.

The largest benchmark performed:

```text
10,000 hashes
49,995,000 pairwise comparisons

Python: 7.467520 s
C++20:  0.080851 s

Speedup: 92.36x
```

The benchmark deliberately keeps the algorithm identical in Python and C++ so that the comparison measures implementation-level performance rather than a change in asymptotic complexity.

---

## Architecture

```text
                    ┌────────────────────┐
                    │   Parquet Dataset  │
                    └─────────┬──────────┘
                              │
                              ▼
                ┌──────────────────────────┐
                │ Python Async Extraction  │
                │   aiohttp + asyncio      │
                └─────────────┬────────────┘
                              │
                              ▼
                ┌──────────────────────────┐
                │ Image Download / Decode  │
                │   Pillow + ImageHash     │
                └─────────────┬────────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │ 64-bit pHashes  │
                    │   hashes.tsv    │
                    └────────┬────────┘
                             │
              ┌──────────────┴──────────────┐
              │                             │
              ▼                             ▼
    ┌───────────────────┐        ┌────────────────────────┐
    │  Python Baseline  │        │ C++20 Clustering Core  │
    │                   │        │                        │
    │ XOR + bit_count() │        │ XOR + std::popcount   │
    └─────────┬─────────┘        └────────────┬───────────┘
              │                              │
              └──────────────┬───────────────┘
                             │
                             ▼
                ┌──────────────────────────┐
                │ DSU / Union-Find         │
                │ Path compression         │
                │ Union by size            │
                └─────────────┬────────────┘
                              │
                              ▼
                     ┌─────────────────┐
                     │ Logo Clusters   │
                     └─────────────────┘
```

The Python extraction stage and C++ clustering stage communicate through a simple TSV interchange format, keeping the performance-critical component independent from the network-bound extraction pipeline.

---

## Python Extraction Pipeline

Web extraction is primarily I/O-bound, so the Python stage uses `asyncio` and `aiohttp` rather than processing domains sequentially.

Concurrency is explicitly bounded to avoid creating an uncontrolled number of simultaneous HTTP connections.

For each domain, the extractor attempts several strategies:

1. OpenGraph metadata such as `og:image`.
2. Standard icons and `apple-touch-icon` resources.
3. Heuristic inspection of `<img>` elements for identifiers associated with logos, brands, and navigation elements.

Successfully downloaded images are validated and decoded before perceptual hashing.

Image decoding and hash generation are CPU-bound operations, so they are moved away from the main asyncio event loop with `asyncio.to_thread()`.

The resulting domain/hash pairs are written to:

```text
hashes.tsv
```

---

## Perceptual Hashing

Traditional cryptographic hashes are unsuitable for visual similarity because even a tiny image modification produces a completely different digest.

Instead, the pipeline uses **64-bit perceptual hashes (pHash)** through the `ImageHash` library.

Visually similar images tend to produce hashes with small Hamming distances even when the images differ in dimensions, compression, or encoding.

For two hashes `A` and `B`, similarity is evaluated using:

```text
HammingDistance(A, B)
```

with a clustering threshold of:

```text
distance <= 8
```

---

## C++20 Clustering Core

The original clustering implementation was written in Python.

Profiling the architecture showed that, unlike web extraction, the pairwise similarity stage is CPU-bound. I therefore implemented an equivalent clustering core in **C++20** while preserving the same algorithm and clustering semantics.

Each perceptual hash is represented as:

```cpp
std::uint64_t
```

Hamming distance is computed using:

```cpp
std::popcount(hash_a ^ hash_b)
```

The core then uses Disjoint Set Union to construct connected components of visually related logos.

### Union-Find optimizations

The DSU implementation uses:

- path compression;
- union by size.

This gives approximately:

```text
O(α(N))
```

amortized complexity for individual DSU operations, where `α` is the inverse Ackermann function.

The expensive part of the current implementation is therefore not DSU itself, but discovering similar pairs.

---

## Correctness Verification

Performance improvements are only useful if they preserve behavior.

The repository therefore includes:

```text
verify_clusters.py
```

which compares cluster memberships generated by Python and C++.

It does not merely compare the number of clusters. Each cluster is converted into a set of members and matched against the equivalent output from the other implementation.

For the real 188-hash dataset:

```text
Python clusters: 144
C++ clusters:    144

[PASS] Outputs are logically identical.
Every Python cluster has exactly the same members as its C++ counterpart.
```

Both implementations also reported:

```text
Pairwise comparisons:  17,578
Successful DSU unions: 44
```

---

## Benchmarking

To isolate clustering performance from network latency and image processing, the repository includes a deterministic synthetic benchmark.

The benchmark generates 64-bit hashes containing both random samples and controlled near-duplicates.

Both implementations receive:

- identical hashes;
- the same Hamming threshold;
- the same O(N²) comparison strategy;
- equivalent Union-Find behavior.

Each workload is executed three times and the **median clustering runtime** is reported.

| Hashes | Pairwise comparisons | Python | C++20 | Speedup |
|------:|---------------------:|-------:|-------:|--------:|
| 500 | 124,750 | 0.016223 s | 0.000216 s | 75.11x |
| 1,000 | 499,500 | 0.060630 s | 0.000861 s | 70.42x |
| 2,500 | 3,123,750 | 0.362623 s | 0.005347 s | 67.82x |
| 5,000 | 12,497,500 | 1.269031 s | 0.018999 s | 66.79x |
| 10,000 | 49,995,000 | 7.467520 s | 0.080851 s | 92.36x |

Raw results are committed in:

```text
benchmarks/benchmark_results.csv
```

The important distinction is that moving from Python to C++ improves the constant factors but **does not change the O(N²) similarity-search complexity**.

That bottleneck is intentionally documented rather than hidden.

---

## Multithreaded Similarity Evaluation

The C++ core also supports multiple worker threads.

Rather than allowing several threads to mutate the same Union-Find structure concurrently, workers perform Hamming-distance comparisons independently and accumulate candidate similarity edges.

After the workers finish, those edges are merged through a single DSU.

```text
Hashes
  │
  ▼
Parallel Hamming Comparisons
  │
  ├── Worker 1
  ├── Worker 2
  ├── ...
  └── Worker N
  │
  ▼
Candidate Similarity Edges
  │
  ▼
Single-threaded DSU Merge
  │
  ▼
Clusters
```

This design avoids concurrent mutation of DSU state and therefore avoids introducing races into the cluster construction step.

Single-threaded and 8-thread executions were both verified against the Python implementation and produced identical cluster memberships.

Parallel execution is not automatically faster for small inputs: with only 188 hashes, thread-management overhead is larger than the comparison workload itself. The parallel mode is intended for workloads large enough to amortize that overhead.

---

## Testing

The C++ implementation is separated into a reusable clustering library and a CLI executable so that the core behavior can be tested independently.

Tests are implemented using **GoogleTest** and executed through **CTest**.

Current tests cover:

- identical-hash Hamming distance;
- single-bit differences;
- full 64-bit differences;
- hexadecimal hash parsing;
- invalid hash input;
- DSU connectivity;
- repeated union operations;
- threshold-based clustering;
- transitive connected components;
- invalid clustering thresholds.

Run locally:

```bash
cmake -S cpp -B cpp/build -DBUILD_TESTING=ON
cmake --build cpp/build --config Release
ctest --test-dir cpp/build -C Release --output-on-failure
```

---

## Continuous Integration

Every push and pull request runs automated C++ validation through GitHub Actions.

The CI matrix currently includes:

```text
Windows
└── Build + GoogleTest/CTest

Ubuntu
└── Build + GoogleTest/CTest

Ubuntu Sanitized Build
├── AddressSanitizer
└── UndefinedBehaviorSanitizer
```

This validates the C++ implementation across both MSVC and Linux toolchains and checks for common memory and undefined-behavior errors.

---

## Build and Run

### Requirements

Python dependencies:

```bash
pip install -r requirements.txt
```

C++ requirements:

```text
C++20-compatible compiler
CMake 3.20+
```

The raw Parquet dataset is intentionally not committed to the repository.

Place it in the project root as:

```text
logos.snappy.parquet
```

### Run the Python pipeline

```bash
python main.py
```

This generates the runtime artifacts:

```text
hashes.tsv
results.json
```

These generated files are intentionally excluded from version control.

### Build the C++ core

```bash
cmake -S cpp -B cpp/build -DBUILD_TESTING=ON
cmake --build cpp/build --config Release
```

### Run on Windows

Single-threaded:

```powershell
.\cpp\build\Release\logo_clusterer.exe hashes.tsv clusters_cpp.tsv 8 1
```

Eight workers:

```powershell
.\cpp\build\Release\logo_clusterer.exe hashes.tsv clusters_cpp.tsv 8 8
```

### Run on Linux

For a standard single-configuration CMake generator:

```bash
./cpp/build/logo_clusterer hashes.tsv clusters_cpp.tsv 8 1
```

### Verify Python vs C++

```bash
python verify_clusters.py
```

### Run the benchmark

Build the C++ core first, then run:

```bash
python benchmarks/benchmark.py
```

---

## Docker

The Python extraction environment can also be built using Docker:

```bash
docker compose up --build
```

Containerization keeps the Python and native image-processing dependencies isolated from the host environment.

Docker is used for reproducibility of the extraction environment; it is not required to build the standalone C++ clustering core.

---

## Project Structure

```text
.
├── .github/
│   └── workflows/
│       └── cpp-ci.yml
│
├── benchmarks/
│   ├── benchmark.py
│   └── benchmark_results.csv
│
├── cpp/
│   ├── include/
│   │   ├── clustering.hpp
│   │   └── dsu.hpp
│   │
│   ├── src/
│   │   ├── clustering.cpp
│   │   └── main.cpp
│   │
│   ├── tests/
│   │   └── test_clustering.cpp
│   │
│   └── CMakeLists.txt
│
├── Dockerfile
├── docker-compose.yml
├── main.py
├── requirements.txt
├── verify_clusters.py
├── .gitignore
├── LICENSE
└── README.md
```

---

## Complexity

For `N` successfully extracted hashes:

| Stage | Complexity / characteristic |
|---|---|
| Web extraction | Network-bound, concurrent |
| Perceptual hashing | Linear in processed images |
| All-pairs similarity search | **O(N²)** |
| 64-bit Hamming distance | **O(1)** |
| DSU `find` / `union` | **O(α(N)) amortized** |
| DSU storage | **O(N)** |
| Multithreaded all-pairs mode | **O(N²)** total work |

Moving the clustering stage to C++ dramatically reduces runtime in the tested workloads, but does not solve the quadratic scaling of all-pairs similarity search.

---

## Design Decisions

### Why Python for extraction?

The extraction stage spends most of its time waiting for network responses.

Python provides mature asynchronous HTTP, HTML parsing, image-processing, and Parquet tooling while keeping the I/O orchestration concise.

### Why C++ for clustering?

The similarity stage repeatedly operates on fixed-width 64-bit values and is CPU-bound.

C++20 provides efficient bitwise operations, `std::popcount`, low iteration overhead, explicit memory behavior, and native multithreading.

### Why not rewrite everything in C++?

The network-bound extraction stage was not the same type of bottleneck as pairwise clustering.

Keeping asynchronous extraction in Python and moving the compute-heavy stage to C++ lets each language address the part of the system where it provides the greatest benefit.

---

## Limitations and Future Work

### Quadratic similarity search

The current implementation still checks every pair of hashes:

```text
N(N - 1) / 2
```

This becomes the dominant scaling bottleneck at large `N`.

A future optimization is to introduce candidate generation or indexing in Hamming space so that only plausible neighbors require an exact Hamming-distance check.

Any such optimization should be validated against the existing O(N²) implementation to ensure that clustering semantics are preserved.

### Web extraction

Real-world extraction remains affected by:

- unavailable or parked domains;
- rate limiting and anti-bot systems;
- JavaScript-rendered assets;
- unusual page structures;
- malformed or unsupported images;
- network latency and transient failures.

A browser-rendering fallback could improve coverage for JavaScript-heavy sites, but would increase resource usage substantially.

### Parallel execution

Multithreading introduces scheduling and synchronization overhead.

It is useful only when the comparison workload is large enough to amortize those costs. Characterizing that crossover point is a natural extension of the current benchmark suite.

---

## What This Project Demonstrates

The project evolved from a Python web-extraction prototype into a small performance-engineering system covering:

- asynchronous and bounded I/O;
- perceptual hashing;
- bitwise similarity computation;
- connected-component construction with Union-Find;
- C++20 performance optimization;
- multithreaded computation;
- deterministic benchmarking;
- cross-implementation correctness validation;
- unit testing;
- CMake;
- cross-platform continuous integration;
- AddressSanitizer and UndefinedBehaviorSanitizer;
- Docker-based environment reproducibility.

The next major scaling improvement is algorithmic rather than language-level: reducing the number of candidate comparisons before exact Hamming-distance evaluation.

---

## License

MIT License.