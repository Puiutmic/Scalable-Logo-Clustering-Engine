#include <chrono>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>

#include "clustering.hpp"


int main(
    int argc,
    char* argv[]
)
{
    try {

        if (
            argc < 3
            || argc > 5
            ) {

            std::cerr
                << "Usage:\n"
                << "  logo_clusterer "
                << "<hashes.tsv> "
                << "<clusters.tsv> "
                << "[threshold] "
                << "[threads]\n";

            return 1;
        }


        const std::string input_path =
            argv[1];

        const std::string output_path =
            argv[2];


        int threshold = 8;

        if (argc >= 4) {
            threshold =
                std::stoi(
                    argv[3]
                );
        }


        std::size_t thread_count = 1;

        if (argc == 5) {
            thread_count =
                static_cast<std::size_t>(
                    std::stoul(
                        argv[4]
                    )
                    );
        }


        const auto records =
            load_hashes(
                input_path
            );


        if (records.empty()) {
            throw std::runtime_error(
                "Input file contains "
                "no hashes."
            );
        }


        const auto start =
            std::chrono::steady_clock::now();


        ClusteringResult result;


        if (thread_count == 1) {

            result =
                cluster_records(
                    records,
                    threshold
                );

        }
        else {

            result =
                cluster_records_parallel(
                    records,
                    threshold,
                    thread_count
                );
        }


        const auto end =
            std::chrono::steady_clock::now();


        save_clusters(
            output_path,
            result.groups
        );


        const double seconds =
            std::chrono::duration<double>(
                end - start
            ).count();


        std::cout
            << "========================================\n"
            << "C++20 LOGO CLUSTERING CORE\n"
            << "========================================\n";


        std::cout
            << "Hashes processed:       "
            << records.size()
            << '\n';


        std::cout
            << "Threads:                "
            << thread_count
            << '\n';


        std::cout
            << "Pairwise comparisons:   "
            << result.comparisons
            << '\n';


        std::cout
            << "Successful DSU unions:  "
            << result.successful_unions
            << '\n';


        std::cout
            << "Clusters generated:     "
            << result.groups.size()
            << '\n';


        std::cout
            << std::fixed
            << std::setprecision(6);


        std::cout
            << "C++ clustering:         "
            << seconds
            << "s\n";


        std::cout
            << "Output written to:      "
            << output_path
            << '\n';


        std::cout
            << "========================================\n";


        return 0;
    }

    catch (
        const std::exception& error
        ) {

        std::cerr
            << "[ERROR] "
            << error.what()
            << '\n';

        return 1;
    }
}