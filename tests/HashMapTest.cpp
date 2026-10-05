#include "../src/server/HashMap.hpp"

#include <gtest/gtest.h>

#include <string>

using treenity::HashMap;

namespace {

// Forces every key into the same bucket regardless of bucket_count(), so
// tests built on it exercise the chaining collision path deterministically
// instead of hoping FNV-1a happens to collide.
struct ConstantHash {
    std::size_t operator()(const std::string&) const { return 42; }
};

}

TEST(HashMap, InsertAndFind) {
    HashMap<std::string, int> map;
    EXPECT_TRUE(map.insert("a", 1));
    ASSERT_NE(map.find("a"), nullptr);
    EXPECT_EQ(*map.find("a"), 1);
    EXPECT_EQ(map.size(), 1u);
}

TEST(HashMap, FindMissingKeyReturnsNull) {
    HashMap<std::string, int> map;
    EXPECT_EQ(map.find("missing"), nullptr);
}

TEST(HashMap, DuplicateInsertReturnsFalseAndKeepsOriginalValue) {
    HashMap<std::string, int> map;
    EXPECT_TRUE(map.insert("a", 1));
    EXPECT_FALSE(map.insert("a", 2));
    ASSERT_NE(map.find("a"), nullptr);
    EXPECT_EQ(*map.find("a"), 1);
}

TEST(HashMap, UpsertInsertsWhenAbsent) {
    HashMap<std::string, int> map;
    map.upsert("a", 1);
    ASSERT_NE(map.find("a"), nullptr);
    EXPECT_EQ(*map.find("a"), 1);
}

TEST(HashMap, UpsertOverwritesWhenPresent) {
    HashMap<std::string, int> map;
    map.upsert("a", 1);
    map.upsert("a", 2);
    ASSERT_NE(map.find("a"), nullptr);
    EXPECT_EQ(*map.find("a"), 2);
    EXPECT_EQ(map.size(), 1u);
}

TEST(HashMap, EraseRemovesEntry) {
    HashMap<std::string, int> map;
    map.insert("a", 1);
    EXPECT_TRUE(map.erase("a"));
    EXPECT_EQ(map.find("a"), nullptr);
    EXPECT_EQ(map.size(), 0u);
}

TEST(HashMap, EraseUnknownKeyReturnsFalse) {
    HashMap<std::string, int> map;
    EXPECT_FALSE(map.erase("missing"));
}

TEST(HashMap, ChainedCollisionsAreAllIndividuallyFindable) {
    // Every key below hashes to the same bucket, so this only passes if
    // the chain (collision resolution) actually walks to the right node
    // instead of returning the first (or a wrong) entry.
    HashMap<std::string, int, ConstantHash> map(/*initial_buckets=*/4);
    for (int i = 0; i < 20; ++i)
        EXPECT_TRUE(map.insert("key" + std::to_string(i), i));

    for (int i = 0; i < 20; ++i) {
        int* value = map.find("key" + std::to_string(i));
        ASSERT_NE(value, nullptr) << "key" << i;
        EXPECT_EQ(*value, i);
    }
    EXPECT_EQ(map.size(), 20u);
}

TEST(HashMap, EraseUnderCollisionsOnlyRemovesTargetedKey) {
    HashMap<std::string, int, ConstantHash> map(4);
    map.insert("a", 1);
    map.insert("b", 2);
    map.insert("c", 3);

    EXPECT_TRUE(map.erase("b"));
    EXPECT_EQ(map.find("b"), nullptr);
    ASSERT_NE(map.find("a"), nullptr);
    EXPECT_EQ(*map.find("a"), 1);
    ASSERT_NE(map.find("c"), nullptr);
    EXPECT_EQ(*map.find("c"), 3);
}

TEST(HashMap, GrowsUnderLoadAndKeepsEveryEntryFindable) {
    HashMap<std::string, int> map(/*initial_buckets=*/4);
    constexpr int kCount = 200;
    for (int i = 0; i < kCount; ++i)
        map.upsert("key" + std::to_string(i), i);

    EXPECT_EQ(map.size(), static_cast<std::size_t>(kCount));
    EXPECT_GT(map.bucket_count(), 4u);
    for (int i = 0; i < kCount; ++i) {
        int* value = map.find("key" + std::to_string(i));
        ASSERT_NE(value, nullptr) << "key" << i;
        EXPECT_EQ(*value, i);
    }
}
