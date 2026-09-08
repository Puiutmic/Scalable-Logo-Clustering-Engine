#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <vector>


struct LogoRecord {
    std::string domain;
    std::uint64_t hash;
};


using ClusterMap =
std::map<
    std::size_t,
    std::vector<std::string>
>;


struct ClusteringResult {
    ClusterMap groups;

    std::uint64_t comparisons = 0;
    std::uint64_t successful_unions = 0;
};


std::uint64_t parse_hash(
    const std::string& hash_hex
);


int hamming_distance(
    std::uint64_t a,
    std::uint64_t b
);


std::vector<LogoRecord> load_hashes(
    const std::string& input_path
);


ClusteringResult cluster_records(
    const std::vector<LogoRecord>& records,
    int threshold
);


ClusteringResult cluster_records_parallel(
    const std::vector<LogoRecord>& records,
    int threshold,
    std::size_t thread_count
);


void save_clusters(
    const std::string& output_path,
    const ClusterMap& groups
);