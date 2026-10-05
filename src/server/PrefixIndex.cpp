#include "PrefixIndex.hpp"

#include <algorithm>

namespace treenity {

void PrefixIndex::erase_client_id(std::vector<std::string>& ids, const std::string& client_id) {
    ids.erase(std::remove(ids.begin(), ids.end(), client_id), ids.end());
}

void PrefixIndex::add(const ConsumerHandle& consumer) {
    remove(consumer.client_id);
    by_client_[consumer.client_id] = consumer;

    if (consumer.prefix.empty()) {
        wildcard_client_ids_.push_back(consumer.client_id);
        return;
    }

    TrieNode* node = root_.get();
    for (char c : consumer.prefix) {
        std::unique_ptr<TrieNode>& child = node->children[c];
        if (!child)
            child = std::make_unique<TrieNode>();
        node = child.get();
    }
    node->client_ids.push_back(consumer.client_id);
}

void PrefixIndex::remove(const std::string& client_id) {
    auto it = by_client_.find(client_id);
    if (it == by_client_.end())
        return;
    const std::string& prefix = it->second.prefix;

    if (prefix.empty()) {
        erase_client_id(wildcard_client_ids_, client_id);
    } else {
        TrieNode* node = root_.get();
        for (char c : prefix) {
            auto child_it = node->children.find(c);
            if (child_it == node->children.end()) {
                node = nullptr;
                break;
            }
            node = child_it->second.get();
        }
        if (node)
            erase_client_id(node->client_ids, client_id);
    }
    by_client_.erase(it);
}

std::vector<ConsumerHandle> PrefixIndex::match(const std::string& key) const {
    std::vector<ConsumerHandle> result;
    result.reserve(wildcard_client_ids_.size());
    for (const auto& client_id : wildcard_client_ids_)
        result.push_back(by_client_.at(client_id));

    const TrieNode* node = root_.get();
    for (char c : key) {
        auto child_it = node->children.find(c);
        if (child_it == node->children.end())
            break;
        node = child_it->second.get();
        for (const auto& client_id : node->client_ids)
            result.push_back(by_client_.at(client_id));
    }
    return result;
}

std::vector<ConsumerHandle> PrefixIndex::all() const {
    std::vector<ConsumerHandle> result;
    result.reserve(by_client_.size());
    for (const auto& entry : by_client_)
        result.push_back(entry.second);
    return result;
}
}
