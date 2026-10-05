#ifndef TREENITY_PREFIX_INDEX_HPP
#define TREENITY_PREFIX_INDEX_HPP

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace treenity {
struct ConsumerHandle {
    std::string client_id;
    std::string prefix;
    std::string ipc_path;
};

// Trie (prefix tree) over the consumer prefixes registered on a topic.
// match(key) walks the trie one character of `key` at a time and collects
// every node crossed along the way, so a consumer registered on "user"
// matches both "user" and "user.login" but not "admin", in O(key length)
// rather than scanning every consumer. Empty-prefix consumers never touch
// the trie: they are tracked separately and always match (subject VI.9,
// "Direct Addition").
class PrefixIndex {
public:
    void add(const ConsumerHandle& consumer);
    void remove(const std::string& client_id);
    std::vector<ConsumerHandle> match(const std::string& key) const;
    std::vector<ConsumerHandle> all() const;

private:
    struct TrieNode {
        std::unordered_map<char, std::unique_ptr<TrieNode>> children;
        std::vector<std::string> client_ids;
    };

    static void erase_client_id(std::vector<std::string>& ids, const std::string& client_id);

    std::unique_ptr<TrieNode> root_ = std::make_unique<TrieNode>();
    std::vector<std::string> wildcard_client_ids_;
    std::unordered_map<std::string, ConsumerHandle> by_client_;
};
}

#endif // TREENITY_PREFIX_INDEX_HPP
