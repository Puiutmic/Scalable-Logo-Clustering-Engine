import json
import sys


PYTHON_RESULTS_PATH = "results.json"
CPP_RESULTS_PATH = "clusters_cpp.tsv"


def load_python_clusters(path):
    with open(path, "r", encoding="utf-8") as file:
        data = json.load(file)

    return {
        frozenset(cluster)
        for cluster in data
    }


def load_cpp_clusters(path):
    clusters = set()

    with open(path, "r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            line = line.strip()

            if not line:
                continue

            parts = line.split("\t")

            if len(parts) < 2:
                raise ValueError(
                    f"Malformed C++ output on line {line_number}"
                )

            # parts[0] = cluster_0, cluster_1, ...
            domains = parts[1:]

            clusters.add(
                frozenset(domains)
            )

    return clusters


def main():
    print("=" * 60)
    print("PYTHON vs C++ CLUSTER VERIFICATION")
    print("=" * 60)

    python_clusters = load_python_clusters(
        PYTHON_RESULTS_PATH
    )

    cpp_clusters = load_cpp_clusters(
        CPP_RESULTS_PATH
    )

    print(
        f"Python clusters: {len(python_clusters)}"
    )

    print(
        f"C++ clusters:    {len(cpp_clusters)}"
    )

    only_python = (
        python_clusters
        - cpp_clusters
    )

    only_cpp = (
        cpp_clusters
        - python_clusters
    )

    print()

    if not only_python and not only_cpp:
        print("[PASS] Outputs are logically identical.")
        print(
            "Every Python cluster has exactly the same "
            "members as its C++ counterpart."
        )
        print("=" * 60)
        return 0

    print("[FAIL] Cluster memberships differ.")
    print()

    if only_python:
        print(
            f"Clusters only in Python: "
            f"{len(only_python)}"
        )

        for cluster in list(only_python)[:5]:
            print(
                "  PYTHON:",
                sorted(cluster)
            )

    if only_cpp:
        print(
            f"\nClusters only in C++: "
            f"{len(only_cpp)}"
        )

        for cluster in list(only_cpp)[:5]:
            print(
                "  C++:",
                sorted(cluster)
            )

    print("=" * 60)

    return 1


if __name__ == "__main__":
    sys.exit(main())