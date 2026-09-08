#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clustering.hpp"
#include "dsu.hpp"


TEST(
    HammingDistance,
    IdenticalHashesHaveDistanceZero
)
{
    EXPECT_EQ(
        hamming_distance(
            0x123456789ABCDEF0ULL,
            0x123456789ABCDEF0ULL
        ),
        0
    );
}


TEST(
    HammingDistance,
    SingleDifferentBit
)
{
    EXPECT_EQ(
        hamming_distance(
            0ULL,
            1ULL
        ),
        1
    );
}


TEST(
    HammingDistance,
    AllBitsDifferent
)
{
    EXPECT_EQ(
        hamming_distance(
            0ULL,
            UINT64_MAX
        ),
        64
    );
}


TEST(
    HashParsing,
    Parses64BitHexValue
)
{
    EXPECT_EQ(
        parse_hash(
            "ffffffffffffffff"
        ),
        UINT64_MAX
    );
}


TEST(
    HashParsing,
    RejectsInvalidHex
)
{
    EXPECT_THROW(
        parse_hash(
            "not-a-hash"
        ),
        std::exception
    );
}


TEST(
    DSU,
    UnionCreatesConnectedComponent
)
{
    DisjointSetUnion dsu(4);

    EXPECT_TRUE(
        dsu.unite(0, 1)
    );

    EXPECT_TRUE(
        dsu.unite(1, 2)
    );

    EXPECT_EQ(
        dsu.find(0),
        dsu.find(2)
    );

    EXPECT_NE(
        dsu.find(0),
        dsu.find(3)
    );
}


TEST(
    DSU,
    RepeatedUnionReturnsFalse
)
{
    DisjointSetUnion dsu(2);

    EXPECT_TRUE(
        dsu.unite(0, 1)
    );

    EXPECT_FALSE(
        dsu.unite(0, 1)
    );
}


TEST(
    Clustering,
    GroupsNearHashes
)
{
    const std::vector<LogoRecord>
        records = {

        {
            "a.test",
            0b0000ULL
        },

        {
            "b.test",
            0b0001ULL
        },

        {
            "c.test",
            0b0011ULL
        },

        {
            "d.test",
            0b11110000ULL
        }
    };

    const auto result =
        cluster_records(
            records,
            1
        );

    EXPECT_EQ(
        result.comparisons,
        6
    );

    EXPECT_EQ(
        result.successful_unions,
        2
    );

    EXPECT_EQ(
        result.groups.size(),
        2
    );
}


TEST(
    Clustering,
    ConnectedComponentsAreTransitive
)
{
    /*
        A --1bit--> B --1bit--> C

        A and C differ by 2 bits.

        With threshold 1, they are still in the
        same connected component through B.
    */

    const std::vector<LogoRecord>
        records = {

        {
            "a.test",
            0b0000ULL
        },

        {
            "b.test",
            0b0001ULL
        },

        {
            "c.test",
            0b0011ULL
        }
    };

    const auto result =
        cluster_records(
            records,
            1
        );

    EXPECT_EQ(
        result.groups.size(),
        1
    );
}


TEST(
    Clustering,
    RejectsInvalidThreshold
)
{
    const std::vector<LogoRecord>
        records = {

        {
            "test",
            0ULL
        }
    };

    EXPECT_THROW(
        cluster_records(
            records,
            65
        ),
        std::invalid_argument
    );
}