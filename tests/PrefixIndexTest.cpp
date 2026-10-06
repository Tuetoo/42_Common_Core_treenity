#include "../src/server/PrefixIndex.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <vector>

using treenity::ConsumerHandle;
using treenity::PrefixIndex;

namespace {

bool contains(const std::vector<ConsumerHandle>& result, const std::string& client_id) {
    return std::any_of(result.begin(), result.end(),
                        [&](const ConsumerHandle& c) { return c.client_id == client_id; });
}

ConsumerHandle make_consumer(std::string client_id, std::string prefix) {
    std::string ipc_path = "/some/path." + client_id;
    return ConsumerHandle{std::move(client_id), std::move(prefix), std::move(ipc_path)};
}

}

TEST(PrefixIndex, EmptyIndexMatchesNothing) {
    PrefixIndex index;
    EXPECT_TRUE(index.match("anything").empty());
    EXPECT_TRUE(index.all().empty());
}

TEST(PrefixIndex, ExactMatch) {
    PrefixIndex index;
    index.add(make_consumer("c0", "user"));
    auto result = index.match("user");
    ASSERT_EQ(result.size(), 1u);
    EXPECT_EQ(result[0].client_id, "c0");
}

TEST(PrefixIndex, PrefixMatch) {
    PrefixIndex index;
    index.add(make_consumer("c0", "user"));
    auto result = index.match("user.login");
    ASSERT_EQ(result.size(), 1u);
    EXPECT_EQ(result[0].client_id, "c0");
}

TEST(PrefixIndex, NoMatch) {
    PrefixIndex index;
    index.add(make_consumer("c0", "user"));
    EXPECT_TRUE(index.match("admin").empty());
}

TEST(PrefixIndex, NoMatchOnSharedLetterButDifferentBranch) {
    PrefixIndex index;
    index.add(make_consumer("c0", "user.create"));
    // "user.update" shares the "user." prefix but diverges before the end
    // of the registered prefix, so it must not match.
    EXPECT_TRUE(index.match("user.update").empty());
}

TEST(PrefixIndex, EmptyPrefixMatchesEverything) {
    PrefixIndex index;
    index.add(make_consumer("wildcard", ""));
    EXPECT_TRUE(contains(index.match("anything.at.all"), "wildcard"));
    EXPECT_TRUE(contains(index.match(""), "wildcard"));
}

TEST(PrefixIndex, MultipleConsumersCanShareAPrefix) {
    PrefixIndex index;
    index.add(make_consumer("c0", "user"));
    index.add(make_consumer("c1", "user"));
    auto result = index.match("user.create");
    EXPECT_EQ(result.size(), 2u);
    EXPECT_TRUE(contains(result, "c0"));
    EXPECT_TRUE(contains(result, "c1"));
}

TEST(PrefixIndex, MatchCollectsEveryPrefixAlongThePath) {
    PrefixIndex index;
    index.add(make_consumer("short", "user"));
    index.add(make_consumer("long", "user.create"));
    auto result = index.match("user.create");
    EXPECT_EQ(result.size(), 2u);
    EXPECT_TRUE(contains(result, "short"));
    EXPECT_TRUE(contains(result, "long"));

    // Only the shorter prefix applies to a key that stops short of "long"'s.
    auto partial = index.match("user.update");
    EXPECT_EQ(partial.size(), 1u);
    EXPECT_TRUE(contains(partial, "short"));
}

TEST(PrefixIndex, RemoveStopsFutureMatches) {
    PrefixIndex index;
    index.add(make_consumer("c0", "user"));
    index.remove("c0");
    EXPECT_TRUE(index.match("user").empty());
    EXPECT_TRUE(index.all().empty());
}

TEST(PrefixIndex, RemoveIsSafeForUnknownClient) {
    PrefixIndex index;
    EXPECT_NO_THROW(index.remove("never-added"));
}

TEST(PrefixIndex, RemoveOnlyAffectsTargetedClient) {
    PrefixIndex index;
    index.add(make_consumer("c0", "user"));
    index.add(make_consumer("c1", "user"));
    index.remove("c0");
    auto result = index.match("user");
    ASSERT_EQ(result.size(), 1u);
    EXPECT_EQ(result[0].client_id, "c1");
}

TEST(PrefixIndex, ReAddingReplacesThePreviousPrefix) {
    PrefixIndex index;
    index.add(make_consumer("c0", "user"));
    index.add(make_consumer("c0", "admin"));
    EXPECT_TRUE(index.match("user").empty());
    EXPECT_TRUE(contains(index.match("admin"), "c0"));
    EXPECT_EQ(index.all().size(), 1u);
}

TEST(PrefixIndex, AllReturnsEveryActiveConsumerRegardlessOfPrefix) {
    PrefixIndex index;
    index.add(make_consumer("c0", "user"));
    index.add(make_consumer("c1", ""));
    index.add(make_consumer("c2", "admin"));
    auto all = index.all();
    EXPECT_EQ(all.size(), 3u);
    EXPECT_TRUE(contains(all, "c0"));
    EXPECT_TRUE(contains(all, "c1"));
    EXPECT_TRUE(contains(all, "c2"));
}

TEST(PrefixIndex, EmptyKeyOnlyMatchesEmptyOrWildcardPrefixes) {
    PrefixIndex index;
    index.add(make_consumer("exact-empty", ""));
    index.add(make_consumer("non-empty", "user"));
    auto result = index.match("");
    ASSERT_EQ(result.size(), 1u);
    EXPECT_EQ(result[0].client_id, "exact-empty");
}
