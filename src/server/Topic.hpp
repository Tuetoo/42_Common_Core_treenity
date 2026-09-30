#ifndef TREENITY_TOPIC_HPP
#define TREENITY_TOPIC_HPP

#include <cstdint>
#include <optional>
#include <shared_mutex>
#include <string>
#include <vector>

namespace treenity {
struct StoredMessage {
    uint32_t   offset;
    std::string key;
    std::string value;
};

class Topic {
public:
    explicit Topic(std::string name);
    const std::string& name() const;
    uint32_t append(std::string key, std::string value);
    std::optional<StoredMessage> get(uint32_t offset) const;
    uint32_t size() const;

private:
    std::string name_;
    mutable std::shared_mutex log_mutex_;
    std::vector<StoredMessage> log_;
};
}

#endif // TREENITY_TOPIC_HPP
