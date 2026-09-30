#include "Topic.hpp"

#include <mutex>
#include <utility>

namespace treenity {

Topic::Topic(std::string name) : name_(std::move(name)) {}

const std::string& Topic::name() const {
    return name_;
}

uint32_t Topic::append(std::string key, std::string value) {
    std::unique_lock lock(log_mutex_);
    uint32_t offset = static_cast<uint32_t>(log_.size());
    log_.push_back(StoredMessage{offset, std::move(key), std::move(value)});
    return offset;
}

std::optional<StoredMessage> Topic::get(uint32_t offset) const {
    std::shared_lock lock(log_mutex_);
    if (offset >= log_.size())
        return std::nullopt;
    return log_[offset];
}

uint32_t Topic::size() const {
    std::shared_lock lock(log_mutex_);
    return static_cast<uint32_t>(log_.size());
}
}