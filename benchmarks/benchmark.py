import csv
import random
import re
import statistics
import subprocess
from pathlib import Path
from time import perf_counter


# ============================================================
# CONFIGURATION
# ============================================================

BENCHMARK_SIZES = [
    500,
    1_000,
    2_500,
    5_000,
    10_000,
]

REPEATS = 3
HAMMING_THRESHOLD = 8
RANDOM_SEED = 20260908


# ============================================================
# PATHS
# ============================================================

BENCHMARK_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BENCHMARK_DIR.parent

RESULTS_CSV = BENCHMARK_DIR / "benchmark_results.csv"

CPP_EXECUTABLE_CANDIDATES = [
    PROJECT_ROOT
    / "cpp"
    / "build"
    / "Release"
    / "logo_clusterer.exe",

    PROJECT_ROOT
    / "cpp"
    / "build"
    / "bin"
    / "logo_clusterer.exe",
]


# ============================================================
# PYTHON DSU
# ============================================================

class DisjointSetUnion:
    def __init__(self, count):
        self.parent = list(range(count))
        self.size = [1] * count

    def find(self, item):
        if self.parent[item] != item:
            self.parent[item] = self.find(
                self.parent[item]
            )

        return self.parent[item]

    def union(self, a, b):
        root_a = self.find(a)
        root_b = self.find(b)

        if root_a == root_b:
            return False

        if self.size[root_a] < self.size[root_b]:
            root_a, root_b = root_b, root_a

        self.parent[root_b] = root_a
        self.size[root_a] += self.size[root_b]

        return True


# ============================================================
# SYNTHETIC DATASET GENERATION
# ============================================================

def mutate_hash(base_hash, rng, bit_changes):
    """
    Flip a fixed number of randomly selected bits in a
    64-bit hash.
    """
    mutated = base_hash

    positions = rng.sample(
        range(64),
        bit_changes
    )

    for position in positions:
        mutated ^= (1 << position)

    return mutated


def generate_hashes(count):
    """
    Generate deterministic synthetic 64-bit perceptual hashes.

    Each block of ~10 hashes contains:
        - 1 random base hash
        - 2 near-duplicates within <=4 bits of the base
        - up to 7 unrelated random hashes

    This ensures that the benchmark exercises both:
        - O(N^2) Hamming-distance comparisons
        - actual DSU union operations

    instead of benchmarking a dataset where every hash is
    unrelated.
    """
    rng = random.Random(
        RANDOM_SEED + count
    )

    hashes = []

    while len(hashes) < count:

        base_hash = rng.getrandbits(64)
        hashes.append(base_hash)

        # Add two near-duplicates.
        for _ in range(2):

            if len(hashes) >= count:
                break

            bit_changes = rng.randint(1, 4)

            hashes.append(
                mutate_hash(
                    base_hash,
                    rng,
                    bit_changes
                )
            )

        # Fill the remainder of this logical block with
        # unrelated hashes.
        for _ in range(7):

            if len(hashes) >= count:
                break

            hashes.append(
                rng.getrandbits(64)
            )

    return hashes


# ============================================================
# PYTHON BASELINE
# ============================================================

def cluster_python(
    hashes,
    threshold
):
    """
    Pure-Python implementation of exactly the same clustering
    logic used by the C++20 core.

    Similarity stage:
        O(N^2) pairwise comparisons

    Hamming distance:
        (a ^ b).bit_count()

    Cluster construction:
        DSU with path compression + union by size
    """
    count = len(hashes)

    dsu = DisjointSetUnion(count)

    comparisons = 0
    successful_unions = 0

    start = perf_counter()

    for i in range(count):

        hash_i = hashes[i]

        for j in range(i + 1, count):

            comparisons += 1

            distance = (
                hash_i ^ hashes[j]
            ).bit_count()

            if distance <= threshold:

                if dsu.union(i, j):
                    successful_unions += 1

    clustering_seconds = (
        perf_counter() - start
    )

    roots = {
        dsu.find(i)
        for i in range(count)
    }

    clusters = len(roots)

    return {
        "seconds": clustering_seconds,
        "comparisons": comparisons,
        "unions": successful_unions,
        "clusters": clusters,
    }


# ============================================================
# C++ EXECUTABLE DISCOVERY
# ============================================================

def find_cpp_executable():

    for candidate in CPP_EXECUTABLE_CANDIDATES:

        if candidate.exists():
            return candidate

    raise FileNotFoundError(
        "Could not find logo_clusterer.exe.\n"
        "Build the C++ project in Release mode first."
    )


# ============================================================
# TSV EXPORT
# ============================================================

def write_hashes_tsv(
    path,
    hashes
):
    with path.open(
        "w",
        encoding="utf-8",
        newline=""
    ) as file:

        for index, value in enumerate(hashes):

            domain = (
                f"benchmark-{index:06d}.test"
            )

            file.write(
                f"{domain}\t{value:016x}\n"
            )


# ============================================================
# C++ OUTPUT PARSING
# ============================================================

def extract_integer(
    pattern,
    text,
    field_name
):
    match = re.search(
        pattern,
        text
    )

    if not match:
        raise RuntimeError(
            f"Could not parse C++ field: "
            f"{field_name}"
        )

    return int(match.group(1))


def extract_float(
    pattern,
    text,
    field_name
):
    match = re.search(
        pattern,
        text
    )

    if not match:
        raise RuntimeError(
            f"Could not parse C++ field: "
            f"{field_name}"
        )

    return float(match.group(1))


# ============================================================
# C++ BENCHMARK
# ============================================================

def cluster_cpp(
    executable,
    input_path,
    output_path,
    threshold
):
    result = subprocess.run(
        [
            str(executable),
            str(input_path),
            str(output_path),
            str(threshold),
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    if result.returncode != 0:

        raise RuntimeError(
            "C++ clusterer failed:\n\n"
            + result.stdout
            + "\n"
            + result.stderr
        )

    output = result.stdout

    comparisons = extract_integer(
        r"Pairwise comparisons:\s+(\d+)",
        output,
        "comparisons"
    )

    unions = extract_integer(
        r"Successful DSU unions:\s+(\d+)",
        output,
        "unions"
    )

    clusters = extract_integer(
        r"Clusters generated:\s+(\d+)",
        output,
        "clusters"
    )

    seconds = extract_float(
        r"C\+\+ clustering:\s+([0-9.]+)s",
        output,
        "clustering time"
    )

    return {
        "seconds": seconds,
        "comparisons": comparisons,
        "unions": unions,
        "clusters": clusters,
    }


# ============================================================
# CORRECTNESS CHECK
# ============================================================

def verify_metrics(
    python_result,
    cpp_result,
    size
):
    fields = [
        "comparisons",
        "unions",
        "clusters",
    ]

    for field in fields:

        if (
            python_result[field]
            != cpp_result[field]
        ):

            raise RuntimeError(
                f"Mismatch at N={size:,} "
                f"for '{field}': "
                f"Python={python_result[field]}, "
                f"C++={cpp_result[field]}"
            )


# ============================================================
# BENCHMARK
# ============================================================

def benchmark_size(
    executable,
    size
):
    print()
    print("=" * 70)
    print(
        f"BENCHMARK N = {size:,}"
    )
    print("=" * 70)

    hashes = generate_hashes(size)

    input_path = (
        BENCHMARK_DIR
        / f"_benchmark_{size}.tsv"
    )

    cpp_output_path = (
        BENCHMARK_DIR
        / f"_clusters_cpp_{size}.tsv"
    )

    write_hashes_tsv(
        input_path,
        hashes
    )

    python_times = []
    cpp_times = []

    expected_comparisons = (
        size * (size - 1) // 2
    )

    final_python_result = None
    final_cpp_result = None

    for run in range(
        1,
        REPEATS + 1
    ):

        print(
            f"\nRun {run}/{REPEATS}"
        )

        # ----------------------------------------------------
        # Python
        # ----------------------------------------------------

        python_result = cluster_python(
            hashes,
            HAMMING_THRESHOLD
        )

        python_times.append(
            python_result["seconds"]
        )

        # ----------------------------------------------------
        # C++
        # ----------------------------------------------------

        cpp_result = cluster_cpp(
            executable,
            input_path,
            cpp_output_path,
            HAMMING_THRESHOLD
        )

        cpp_times.append(
            cpp_result["seconds"]
        )

        # ----------------------------------------------------
        # Validate
        # ----------------------------------------------------

        verify_metrics(
            python_result,
            cpp_result,
            size
        )

        if (
            python_result["comparisons"]
            != expected_comparisons
        ):
            raise RuntimeError(
                f"Unexpected comparison count "
                f"for N={size:,}"
            )

        final_python_result = python_result
        final_cpp_result = cpp_result

        print(
            f"Python: "
            f"{python_result['seconds']:.6f}s"
        )

        print(
            f"C++:    "
            f"{cpp_result['seconds']:.6f}s"
        )

        print(
            "[PASS] Metrics match."
        )

    # --------------------------------------------------------
    # Median
    # --------------------------------------------------------

    python_median = statistics.median(
        python_times
    )

    cpp_median = statistics.median(
        cpp_times
    )

    if cpp_median > 0:
        speedup = (
            python_median
            / cpp_median
        )
    else:
        speedup = float("inf")

    print()
    print(
        f"Median Python: "
        f"{python_median:.6f}s"
    )

    print(
        f"Median C++:    "
        f"{cpp_median:.6f}s"
    )

    print(
        f"Speedup:       "
        f"{speedup:.2f}x"
    )

    print(
        f"Comparisons:   "
        f"{expected_comparisons:,}"
    )

    print(
        f"Clusters:      "
        f"{final_python_result['clusters']:,}"
    )

    # --------------------------------------------------------
    # Cleanup temporary files
    # --------------------------------------------------------

    if input_path.exists():
        input_path.unlink()

    if cpp_output_path.exists():
        cpp_output_path.unlink()

    return {
        "n": size,
        "comparisons": expected_comparisons,
        "clusters": final_python_result[
            "clusters"
        ],
        "unions": final_python_result[
            "unions"
        ],
        "python_seconds": python_median,
        "cpp_seconds": cpp_median,
        "speedup": speedup,
    }


# ============================================================
# CSV OUTPUT
# ============================================================

def save_results_csv(results):

    fieldnames = [
        "n",
        "comparisons",
        "clusters",
        "unions",
        "python_seconds",
        "cpp_seconds",
        "speedup",
    ]

    with RESULTS_CSV.open(
        "w",
        encoding="utf-8",
        newline=""
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=fieldnames
        )

        writer.writeheader()

        for row in results:
            writer.writerow(row)


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 70)
    print("PYTHON vs C++20 CLUSTERING BENCHMARK")
    print("=" * 70)

    executable = find_cpp_executable()

    print(
        f"\nC++ executable:\n"
        f"{executable}"
    )

    print(
        f"\nThreshold: {HAMMING_THRESHOLD}"
    )

    print(
        f"Repeats per size: {REPEATS}"
    )

    results = []

    for size in BENCHMARK_SIZES:

        result = benchmark_size(
            executable,
            size
        )

        results.append(result)

    save_results_csv(
        results
    )

    print()
    print("=" * 70)
    print("FINAL RESULTS")
    print("=" * 70)

    print(
        f"{'N':>8} "
        f"{'Comparisons':>15} "
        f"{'Python (s)':>14} "
        f"{'C++ (s)':>12} "
        f"{'Speedup':>12}"
    )

    print("-" * 70)

    for result in results:

        print(
            f"{result['n']:>8,} "
            f"{result['comparisons']:>15,} "
            f"{result['python_seconds']:>14.6f} "
            f"{result['cpp_seconds']:>12.6f} "
            f"{result['speedup']:>11.2f}x"
        )

    print()
    print(
        f"Results saved to:\n"
        f"{RESULTS_CSV}"
    )

    print("=" * 70)


if __name__ == "__main__":
    main()