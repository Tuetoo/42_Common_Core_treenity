#ifndef TREENITY_PREFIX_INDEX_HPP
#define TREENITY_PREFIX_INDEX_HPP

#include "HashMap.hpp"

#include <map>
#include <memory>
#include <string>
#include <vector>
#include <cstdint>

namespace treenity {
struct ConsumerHandle {
    std::string client_id;
    std::string prefix;
    std::string ipc_path;
    uint32_t    start_offset = 0;
};

class PrefixIndex {
public:
    void add(const ConsumerHandle& consumer);
    void remove(const std::string& client_id);
    std::vector<ConsumerHandle> match(const std::string& key) const;
    std::vector<ConsumerHandle> all() const;

private:
    struct TrieNode {
        std::map<char, std::unique_ptr<TrieNode>> children;
        std::vector<std::string> client_ids;
    };

    static void erase_client_id(std::vector<std::string>& ids, const std::string& client_id);
    void collect_ids(const TrieNode& node, std::vector<ConsumerHandle>& out) const;

    std::unique_ptr<TrieNode> root_ = std::make_unique<TrieNode>();
    std::vector<std::string> wildcard_client_ids_;
    HashMap<std::string, ConsumerHandle> by_client_;
};
}

#endif // TREENITY_PREFIX_INDEX_HPP