#include "clustering.hpp"

#include <algorithm>
#include <atomic>
#include <bit>
#include <fstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "dsu.hpp"


using SimilarPair =
std::pair<
    std::size_t,
    std::size_t
>;


std::uint64_t parse_hash(
    const std::string& hash_hex
)
{
    std::size_t parsed_characters = 0;

    const unsigned long long value =
        std::stoull(
            hash_hex,
            &parsed_characters,
            16
        );

    if (
        parsed_characters
        != hash_hex.size()
        ) {
        throw std::runtime_error(
            "Invalid hexadecimal hash: "
            + hash_hex
        );
    }

    return static_cast<std::uint64_t>(
        value
        );
}


int hamming_distance(
    std::uint64_t a,
    std::uint64_t b
)
{
    return static_cast<int>(
        std::popcount(a ^ b)
        );
}


std::vector<LogoRecord> load_hashes(
    const std::string& input_path
)
{
    std::ifstream input(
        input_path
    );

    if (!input.is_open()) {
        throw std::runtime_error(
            "Could not open input file: "
            + input_path
        );
    }

    std::vector<LogoRecord> records;

    std::string line;
    std::size_t line_number = 0;

    while (
        std::getline(input, line)
        ) {

        ++line_number;

        if (
            !line.empty()
            && line.back() == '\r'
            ) {
            line.pop_back();
        }

        if (line.empty()) {
            continue;
        }

        const std::size_t tab_position =
            line.find('\t');

        if (
            tab_position
            == std::string::npos
            ) {
            throw std::runtime_error(
                "Malformed TSV line "
                + std::to_string(
                    line_number
                )
            );
        }

        const std::string domain =
            line.substr(
                0,
                tab_position
            );

        const std::string hash_hex =
            line.substr(
                tab_position + 1
            );

        if (
            domain.empty()
            || hash_hex.empty()
            ) {
            throw std::runtime_error(
                "Missing domain or hash "
                "on line "
                + std::to_string(
                    line_number
                )
            );
        }

        records.push_back({
            domain,
            parse_hash(hash_hex)
            });
    }

    return records;
}


static ClusterMap build_groups(
    DisjointSetUnion& dsu,
    const std::vector<LogoRecord>& records
)
{
    ClusterMap groups;

    for (
        std::size_t i = 0;
        i < records.size();
        ++i
        ) {

        groups[
            dsu.find(i)
        ].push_back(
            records[i].domain
        );
    }

    return groups;
}


ClusteringResult cluster_records(
    const std::vector<LogoRecord>& records,
    int threshold
)
{
    if (
        threshold < 0
        || threshold > 64
        ) {
        throw std::invalid_argument(
            "Hamming threshold must be "
            "between 0 and 64."
        );
    }

    DisjointSetUnion dsu(
        records.size()
    );

    ClusteringResult result;

    for (
        std::size_t i = 0;
        i < records.size();
        ++i
        ) {

        for (
            std::size_t j = i + 1;
            j < records.size();
            ++j
            ) {

            ++result.comparisons;

            if (
                hamming_distance(
                    records[i].hash,
                    records[j].hash
                )
                <= threshold
                ) {

                if (dsu.unite(i, j)) {
                    ++result
                        .successful_unions;
                }
            }
        }
    }

    result.groups =
        build_groups(
            dsu,
            records
        );

    return result;
}


ClusteringResult cluster_records_parallel(
    const std::vector<LogoRecord>& records,
    int threshold,
    std::size_t thread_count
)
{
    if (
        threshold < 0
        || threshold > 64
        ) {
        throw std::invalid_argument(
            "Hamming threshold must be "
            "between 0 and 64."
        );
    }


    if (thread_count == 0) {
        throw std::invalid_argument(
            "Thread count must be >= 1."
        );
    }


    if (
        records.size() < 2
        || thread_count == 1
        ) {
        return cluster_records(
            records,
            threshold
        );
    }


    thread_count =
        std::min(
            thread_count,
            records.size()
        );


    std::atomic<std::size_t>
        next_index{ 0 };


    std::vector<
        std::vector<SimilarPair>
    > thread_pairs(
        thread_count
    );


    std::vector<std::uint64_t>
        thread_comparisons(
            thread_count,
            0
        );


    std::vector<std::thread>
        workers;

    workers.reserve(
        thread_count
    );


    for (
        std::size_t worker_id = 0;
        worker_id < thread_count;
        ++worker_id
        ) {

        workers.emplace_back(
            [
                &records,
                    threshold,
                    &next_index,
                    &thread_pairs,
                    &thread_comparisons,
                    worker_id
            ]()
            {
                auto& local_pairs =
                    thread_pairs[
                        worker_id
                    ];

                std::uint64_t
                    local_comparisons = 0;


                while (true) {

                    const std::size_t i =
                        next_index
                        .fetch_add(
                            1,
                            std::memory_order_relaxed
                        );

                    if (
                        i >= records.size()
                        ) {
                        break;
                    }


                    for (
                        std::size_t j =
                        i + 1;
                        j < records.size();
                        ++j
                        ) {

                        ++local_comparisons;

                        if (
                            hamming_distance(
                                records[i].hash,
                                records[j].hash
                            )
                            <= threshold
                            ) {
                            local_pairs
                                .emplace_back(
                                    i,
                                    j
                                );
                        }
                    }
                }


                thread_comparisons[
                    worker_id
                ] =
                    local_comparisons;
            }
                );
    }


    for (
        auto& worker
        : workers
        ) {
        worker.join();
    }


    DisjointSetUnion dsu(
        records.size()
    );

    ClusteringResult result;


    for (
        const auto count
        : thread_comparisons
        ) {
        result.comparisons += count;
    }


    /*
        DSU merging is intentionally performed
        after all worker threads have joined.

        This avoids concurrent mutation of the
        DSU structure and therefore avoids
        synchronization overhead and races.
    */

    for (
        const auto& pairs
        : thread_pairs
        ) {

        for (
            const auto& [a, b]
            : pairs
            ) {

            if (
                dsu.unite(a, b)
                ) {
                ++result
                    .successful_unions;
            }
        }
    }


    result.groups =
        build_groups(
            dsu,
            records
        );


    return result;
}


void save_clusters(
    const std::string& output_path,
    const ClusterMap& groups
)
{
    std::ofstream output(
        output_path
    );

    if (!output.is_open()) {
        throw std::runtime_error(
            "Could not open output file: "
            + output_path
        );
    }


    std::size_t cluster_id = 0;


    for (
        const auto& [root, domains]
        : groups
        ) {

        static_cast<void>(
            root
            );


        output
            << "cluster_"
            << cluster_id;


        for (
            const std::string& domain
            : domains
            ) {

            output
                << '\t'
                << domain;
        }


        output << '\n';

        ++cluster_id;
    }
}