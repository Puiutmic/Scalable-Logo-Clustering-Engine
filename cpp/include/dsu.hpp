#pragma once

#include <cstddef>
#include <numeric>
#include <utility>
#include <vector>


class DisjointSetUnion {
private:
    std::vector<std::size_t> parent_;
    std::vector<std::size_t> size_;

public:
    explicit DisjointSetUnion(std::size_t count)
        : parent_(count),
        size_(count, 1)
    {
        std::iota(
            parent_.begin(),
            parent_.end(),
            0
        );
    }

    std::size_t find(std::size_t item)
    {
        if (parent_[item] != item) {
            parent_[item] = find(parent_[item]);
        }

        return parent_[item];
    }

    bool unite(std::size_t a, std::size_t b)
    {
        std::size_t root_a = find(a);
        std::size_t root_b = find(b);

        if (root_a == root_b) {
            return false;
        }

        // Union by size:
        // attach the smaller tree below the larger tree.
        if (size_[root_a] < size_[root_b]) {
            std::swap(root_a, root_b);
        }

        parent_[root_b] = root_a;
        size_[root_a] += size_[root_b];

        return true;
    }

    std::size_t component_size(std::size_t item)
    {
        return size_[find(item)];
    }
};