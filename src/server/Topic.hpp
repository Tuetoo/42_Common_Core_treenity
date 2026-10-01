#ifndef TREENITY_TOPIC_HPP
#define TREENITY_TOPIC_HPP

#include "PrefixIndex.hpp"

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
    enum class Kind { PRODUCE, ADD_CONSUMER, REMOVE_CONSUMER };
    Kind            kind = Kind::PRODUCE;
    std::string     key;
    std::string     value;
    ConsumerHandle  consumer;
    uint32_t        start_offset = 0;
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
    void add_consumer(const std::string& client_id, const std::string& prefix,
                        const std::string& ipc_path, uint32_t start_offset);
    void remove_consumer(const std::string& client_id);

    const std::string& name() const;
    uint32_t append(std::string key, std::string value);
    std::optional<StoredMessage> get(uint32_t offset) const;
    uint32_t size() const;

private:
    void worker_loop();
    void handle_produced_record(const std::string& key, const std::string& value);
    void handle_add_consumer(const ConsumerHandle& consumer, uint32_t start_offset);
    void deliver_to(const ConsumerHandle& consumer, const StoredMessage& msg) const;
    void send_shutdown_to_all_consumers();
    void push_work(TopicWorkItem item);

    std::string                 name_;
    mutable std::shared_mutex   log_mutex_;
    std::vector<StoredMessage>  log_;
    PrefixIndex                 consumers_;
    std::queue<TopicWorkItem>   work_queue_;
    std::mutex                  queue_mutex_;
    std::condition_variable     queue_cv_;
    bool                        running_ = false;
    std::thread                 worker_;
};
}

#endif // TREENITY_TOPIC_HPP
