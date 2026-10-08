#include "Topic.hpp"

#include <utility>
#include <cstring>
#include <cerrno>
#include <fcntl.h>
#include <iostream>
#include <stdexcept>
#include <csignal>
#include <ctime>

namespace treenity {

namespace {
constexpr uint32_t POISON_PILL_KEY_SIZE = 0xFFFFFFFF;

void block_shutdown_signals_on_this_thread() {
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGINT);
    sigaddset(&set, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &set, nullptr);
}

bool key_matches(const std::string& prefix, const std::string& key) {
    return prefix.empty() || key.compare(0, prefix.size(), prefix) == 0;
}
}

Topic::Topic(std::string name, pid_t server_pid) 
    : name_(std::move(name)), server_pid_(server_pid) {}

Topic::~Topic() {
    stop();
}

void Topic::start() {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (running_)
            return;
        data_queue_name_ = topic_data_queue_name(server_pid_, name_);

        struct mq_attr attr {} ;
        attr.mq_flags = 0;
        attr.mq_maxmsg = QUEUE_MAX_MSG_COUNT;
        attr.mq_msgsize = PRODUCE_QUEUE_MAX_MSG_SIZE;
        attr.mq_curmsgs = 0;

        mq_unlink(data_queue_name_.c_str());
        data_mq_ = mq_open(data_queue_name_.c_str(), O_CREAT | O_RDONLY, 0600, &attr);
        if (data_mq_ == static_cast<mqd_t>(-1)) {
            throw std::runtime_error("Topic '" + name_ + "': mq_open(" + data_queue_name_ + ") failed: " + std::strerror(errno));
        }
        running_ = true;
    }
    reader_ = std::thread(&Topic::reader_loop, this);
    worker_ = std::thread(&Topic::worker_loop, this);
}

void Topic::stop() {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (!running_)
            return;
    }
    mqd_t self = mq_open(data_queue_name_.c_str(), O_WRONLY);
    if (self != static_cast<mqd_t>(-1)) {
        ProduceRecord pill{};
        pill.key_size = POISON_PILL_KEY_SIZE;
        mq_send(self, reinterpret_cast<const char*>(&pill), sizeof(pill), 0);
        mq_close(self);
    } else {
        std::cerr << "[topic " << name_ << "] could not open own data queue to send the "
                  << "shutdown poison pill (" << std::strerror(errno) << ")\n";
    }
    if (reader_.joinable())
        reader_.join();
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        running_ = false;
    }
    queue_cv_.notify_all();
    if (worker_.joinable())
        worker_.join();
    mq_close(data_mq_);
    mq_unlink(data_queue_name_.c_str());
    data_mq_ = static_cast<mqd_t>(-1);
}

void Topic::push_work(TopicWorkItem item) {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        work_queue_.push(std::move(item));
    }
    queue_cv_.notify_one();
}

void Topic::enqueue_record(std::string key, std::string value) {
    TopicWorkItem item;
    item.kind = TopicWorkItem::Kind::PRODUCE;
    item.key = std::move(key);
    item.value = std::move(value);
    push_work(std::move(item));
}

void Topic::add_consumer(const std::string& client_id, const std::string& prefix,
                            const std::string& ipc_path, uint32_t start_offset) {
    TopicWorkItem item;
    item.kind = TopicWorkItem::Kind::ADD_CONSUMER;
    item.consumer = ConsumerHandle{client_id, prefix, ipc_path, start_offset};
    item.start_offset = start_offset;
    push_work(std::move(item));
}

void Topic::remove_consumer(const std::string& client_id) {
    TopicWorkItem item;
    item.kind = TopicWorkItem::Kind::REMOVE_CONSUMER;
    item.consumer = ConsumerHandle{client_id, "", ""};
    push_work(std::move(item));
}

void Topic::reader_loop() {
    block_shutdown_signals_on_this_thread();
    while (true) {
        char buf[PRODUCE_QUEUE_MAX_MSG_SIZE];
        ssize_t n = mq_receive(data_mq_, buf, sizeof(buf), nullptr);
        if (n<0) {
            if (errno == EINTR)
                continue;
            break;
        }

        ProduceRecord rec;
        std::memcpy(&rec, buf, sizeof(rec));
        if (rec.key_size == POISON_PILL_KEY_SIZE)
            break;
        if (static_cast<std::size_t>(rec.key_size) + rec.value_size > MAX_KV_LEN) {
            std::cerr << "[topic " << name_ << "] malformed record (sizes exceed "
                      << MAX_KV_LEN << "), skipping\n";
            continue;
        }
        enqueue_record(std::string(rec.data, rec.key_size), std::string(rec.data + rec.key_size, rec.value_size));
    }
}

void Topic::worker_loop() {
    block_shutdown_signals_on_this_thread();
    while (true) {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        queue_cv_.wait(lock, [this] { return !work_queue_.empty() || !running_; });
        if (work_queue_.empty())
            break;
        TopicWorkItem item = std::move(work_queue_.front());
        work_queue_.pop();
        lock.unlock();

        switch (item.kind) {
            case TopicWorkItem::Kind::PRODUCE:
                handle_produced_record(item.key, item.value);
                break;
            case TopicWorkItem::Kind::ADD_CONSUMER:
                handle_add_consumer(item.consumer, item.start_offset);
                break;
            case TopicWorkItem::Kind::REMOVE_CONSUMER:
                consumers_.remove(item.consumer.client_id);
                break;
        }
    }
    send_shutdown_to_all_consumers();
}

void Topic::handle_produced_record(const std::string& key, const std::string& value) {
    uint32_t offset = append(key, value);
    StoredMessage msg{offset, key, value};
    for (const auto& consumer : consumers_.match(key)) {
        if (offset >= consumer.start_offset)
            deliver_to(consumer, msg);
    }
}

void Topic::handle_add_consumer(const ConsumerHandle& consumer, uint32_t start_offset) {
    uint32_t end = size();
    for (uint32_t off = start_offset; off < end; ++off) {
        std::optional<StoredMessage> msg = get(off);
        if (msg && key_matches(consumer.prefix, msg->key))
            deliver_to(consumer, *msg);
    }
    consumers_.add(consumer);
}

void Topic::deliver_to(const ConsumerHandle& consumer, const StoredMessage& msg) const {
    if (msg.key.size() + msg.value.size() > MAX_KV_LEN) {
        std::cerr << "[topic " << name_ << "] message at offset " << msg.offset
                  << " is too large to deliver, skipping\n";
        return;
    }
    mqd_t mq = mq_open(consumer.ipc_path.c_str(), O_WRONLY);
    if (mq == static_cast<mqd_t>(-1)) {
        std::cerr << "[topic " << name_ << "] consumer " << consumer.client_id
                  << " unreachable (" << std::strerror(errno) << "), dropping this delivery\n";
        return;
    }
    ServerToClientMessage out{};
    out.type = MessageType::CONSUMER_MESSAGE;
    out.consumer_msg.type = MessageType::CONSUMER_MESSAGE;
    out.consumer_msg.offset = msg.offset;
    out.consumer_msg.key_size = static_cast<uint32_t>(msg.key.size());
    out.consumer_msg.value_size = static_cast<uint32_t>(msg.value.size());
    std::memcpy(out.consumer_msg.data, msg.key.data(), msg.key.size());
    std::memcpy(out.consumer_msg.data + msg.key.size(), msg.value.data(), msg.value.size());
    struct timespec deadline {};
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_nsec += 200L * 1000000L;
    deadline.tv_sec += deadline.tv_nsec / 1000000000L;
    deadline.tv_nsec %= 1000000000L;
    if (mq_timedsend(mq, reinterpret_cast<const char*>(&out), sizeof(out), 0, &deadline) == -1) {
        std::cerr << "[topic " << name_ << "] mq_send to " <<consumer.client_id
                  << " failed: " << std::strerror(errno) << "\n";
    }
    mq_close(mq);
}

void Topic::send_shutdown_to_all_consumers() {
    ServerToClientMessage out{};
    out.type = MessageType::SHUTDOWN;
    out.consumer_msg.type = MessageType::SHUTDOWN;
    for (const auto& consumer : consumers_.all()) {
        mqd_t mq = mq_open(consumer.ipc_path.c_str(), O_WRONLY | O_NONBLOCK);
        if (mq == static_cast<mqd_t>(-1))
            continue;
        mq_send(mq, reinterpret_cast<const char*>(&out), sizeof(out), 0);
        mq_close(mq);
    }
}

const std::string& Topic::name() const {
    return name_;
}

const std::string& Topic::data_queue_name() const {
    return data_queue_name_;
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

