#ifndef TREENITY_TOPIC_HPP
#define TREENITY_TOPIC_HPP

#include <cstdint>
#include <optional>
#include <shared_mutex>
#include <string>
#include <vector>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>

namespace treenity {
struct StoredMessage {
    uint32_t   offset;
    std::string key;
    std::string value;
};

struct TopicWorkItem {
    std::string key;
    std::string value;
};

class Topic {
public:
    explicit Topic(std::string name);
    ~Topic();

    Topic(const Topic&) = delete;
    Topic& operator=(const Topic&) = delete;

    void start();
    void stop();
    void enqueue_record(std::string key, std::string value);

    const std::string& name() const;
    uint32_t append(std::string key, std::string value);
    std::optional<StoredMessage> get(uint32_t offset) const;
    uint32_t size() const;

private:
    void worker_loop();
    std::string                 name_;
    mutable std::shared_mutex   log_mutex_;
    std::vector<StoredMessage>  log_;
    std::queue<TopicWorkItem>   work_queue_;
    std::mutex                  queue_mutex_;
    std::condition_variable     queue_cv_;
    bool                        running_ = false;
    std::thread                 worker_;
};
}

#endif // TREENITY_TOPIC_HPP
