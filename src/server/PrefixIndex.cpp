#include "PrefixIndex.hpp"
#include <algorithm>

namespace treenity {
void PrefixIndex::add(const ConsumerHandle& consumer) {
    remove(consumer.client_id);
    consumers_.push_back(consumer);
}

void PrefixIndex::remove(const std::string& client_id) {
    consumers_.erase(
        std::remove_if(consumers_.begin(), consumers_.end(),
                        [&](const ConsumerHandle& c) { return c.client_id == client_id; }),
        consumers_.end()
    );
}

std::vector<ConsumerHandle> PrefixIndex::match(const std::string& key) const {
    std::vector<ConsumerHandle> result;
    for (const auto& c : consumers_) {
        if (c.prefix.empty() || key.compare(0, c.prefix.size(), c.prefix) == 0)
            result.push_back(c);
    }
    return result;
}

std::vector<ConsumerHandle> PrefixIndex::all() const {
    return consumers_;
}
}