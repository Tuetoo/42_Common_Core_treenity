#include "Topic.hpp"

#include <utility>

namespace treenity {

Topic::Topic(std::string name) : name_(std::move(name)) {}

Topic::~Topic() {
    stop();
}

void Topic::start() {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (running_)
            return;
        running_ = true;
    }
    worker_ = std::thread(&Topic::worker_loop, this);
}

void Topic::stop() {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (!running_)
            return;
        running_ = false;
    }
    queue_cv_.notify_all();
    if (worker_.joinable())
        worker_.join();
}

void Topic::enqueue_record(std::string key, std::string value) {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        work_queue_.push(TopicWorkItem{std::move(key), std::move(value)});
    }
    queue_cv_.notify_one();
}

void Topic::worker_loop() {
    while (true) {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        queue_cv_.wait(lock, [this] { return !work_queue_.empty() || !running_; });
        if (work_queue_.empty())
            break;
        TopicWorkItem item = std::move(work_queue_.front());
        work_queue_.pop();
        lock.unlock();
        append(std::move(item.key), std::move(item.value));
    }
}

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