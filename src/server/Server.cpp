#include "Server.hpp"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <stdexcept>
#include <unistd.h>
#include <cctype>
#include <optional>

namespace treenity {
namespace {
std::string field_to_string(const char* field, std::size_t capacity) {
    return std::string(field, strnlen(field, capacity));
}
}

Server::Server()
    : pid_(getpid()), main_queue_name_(main_queue_name(pid_)) {
    struct mq_attr attr {};
    attr.mq_flags = 0;
    attr.mq_maxmsg = QUEUE_MAX_MSG_COUNT;
    attr.mq_msgsize = MAIN_QUEUE_MAX_MSG_SIZE;
    attr.mq_curmsgs = 0;
    mq_unlink(main_queue_name_.c_str());
    main_mq_ = mq_open(main_queue_name_.c_str(), O_CREAT | O_RDONLY, 0600, &attr);
    if (main_mq_ == static_cast<mqd_t>(-1)) {
        throw std::runtime_error("mq_open(" + main_queue_name_ + ") failed: " + std::strerror(errno));
    }
}

Server::~Server() {
    if (main_mq_ != static_cast<mqd_t>(-1)) {
        mq_close(main_mq_);
        mq_unlink(main_queue_name_.c_str());
    }
}

void Server::run() {
    std::cout << main_queue_name_ << std::endl;

    while (true) {
        char buf[MAIN_QUEUE_MAX_MSG_SIZE];
        ssize_t n = mq_receive(main_mq_, buf, sizeof(buf), nullptr);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            std::cerr << "server: mq_receive failed: " << std::strerror(errno) << "\n";
            break;
        }
        IpcRequest req;
        std::memcpy(&req, buf, sizeof(req));
        if (!is_valid_id(field_to_string(req.client_id, sizeof(req.client_id)))) {
            std::cerr << "server: dropping request with an invalid client id\n";
            continue;
        }
        switch (req.type) {
            case MessageType::CREATE_TOPIC:     handle_create_topic(req); break;
            case MessageType::LIST_TOPICS:      handle_list_topics(req); break;
            case MessageType::REGISTER_CLIENT:  handle_register_client(req); break;
            case MessageType::PRODUCE_START:    handle_produce_start(req); break;
            case MessageType::INFO:             handle_info(req); break;
            case MessageType::CONSUMER_ACK:     handle_consumer_ack(req); break;
            case MessageType::DISCONNECT:       handle_disconnect(req); break;
            default:
                std::cerr << "server: ignoring unexpected message type "
                          << static_cast<int>(req.type) << " on the main queue\n";
        }
    }
}

bool Server::is_valid_id(const std::string& id) {
    if (id.empty() || id.size() > MAX_ID_LEN)
        return false;
    for (unsigned char c : id) {
        if (!std::isalnum(c) && c != '_' && c != '.' && c != '-')
            return false;
    }
    return true;
}

ServerToClientMessage Server::make_ok(uint64_t request_id) {
    ServerToClientMessage msg{};
    msg.type = MessageType::RESPONSE_OK;
    msg.response.type = MessageType::RESPONSE_OK;
    msg.response.request_id = request_id;
    msg.response.error_code = ErrorCode::NONE;
    return msg;
}

ServerToClientMessage Server::make_error(uint64_t request_id, ErrorCode code, const std::string& message) {
    ServerToClientMessage msg{};
    msg.type = MessageType::RESPONSE_ERROR;
    msg.response.type = MessageType::RESPONSE_ERROR;
    msg.response.request_id = request_id;
    msg.response.error_code = code;
    std::strncpy(msg.response.error_message, message.c_str(), MAX_ERROR_MSG_LEN - 1);
    return msg;
}

void Server::reply(const std::string& client_id, const ServerToClientMessage& msg) const {
    std::string path = client_queue_name(pid_, client_id);
    mqd_t mq = mq_open(path.c_str(), O_WRONLY | O_NONBLOCK);
    if (mq == static_cast<mqd_t>(-1)) {
        std::cerr << "server: client '" << client_id << "' unreachable ("
                  << std::strerror(errno) << ")\n";
        return;
    }
    if (mq_send(mq, reinterpret_cast<const char*>(&msg), sizeof(msg), 0) == -1) {
        std::cerr << "server: mq_send to '" << client_id << "' failed: "
                  << std::strerror(errno) << "\n";
    }
    mq_close(mq);
}

void Server::handle_create_topic(const IpcRequest& req) {
    std::string client_id = field_to_string(req.client_id, sizeof(req.client_id));
    std::string topic_name = field_to_string(req.topic_name, sizeof(req.topic_name));
    if (!is_valid_id(topic_name)) {
        reply(client_id, make_error(req.request_id, ErrorCode::GENERAL, "invalid topic name"));
        return;
    }
    if (topics_.count(topic_name)) {
        reply(client_id, make_error(req.request_id, ErrorCode::TOPIC_ERROR, "topic already exists"));
        return;
    }
    auto topic = std::make_unique<Topic>(topic_name, pid_);
    try {
        topic->start();
    } catch (const std::exception& e) {
        std::cerr << "server: " << e.what() << "\n";
        reply(client_id, make_error(req.request_id, ErrorCode::IPC_ERROR, "failed to create topic"));
        return;
    }
    topics_.emplace(topic_name, std::move(topic));
    ServerToClientMessage ok = make_ok(req.request_id);
    std::strncpy(ok.response.topic_name, topic_name.c_str(), MAX_ID_LEN);
    reply(client_id, ok);
}

void Server::handle_list_topics(const IpcRequest& req) {
    std::string client_id = field_to_string(req.client_id, sizeof(req.client_id));
    std::string joined;
    for (const auto& entry : topics_) {
        std::size_t needed = entry.first.size() + (joined.empty() ? 0 : 1);
        if (joined.size() + needed >= MAX_TOPIC_LIST_LEN)
            break;
        if (!joined.empty())
            joined += ',';
        joined += entry.first;
    }
    ServerToClientMessage ok = make_ok(req.request_id);
    std::strncpy(ok.response.topic_list, joined.c_str(), MAX_TOPIC_LIST_LEN - 1);
    reply(client_id, ok);
}

void Server::handle_register_client(const IpcRequest& req) {
    std::string client_id = field_to_string(req.client_id, sizeof(req.client_id));
    std::string topic_name = field_to_string(req.topic_name, sizeof(req.topic_name));
    std::string prefix = field_to_string(req.prefix, sizeof(req.prefix));
    if (!is_valid_id(topic_name)) {
        reply(client_id, make_error(req.request_id, ErrorCode::GENERAL, "invalid topic name"));
        return;
    }
    auto topic_it = topics_.find(topic_name);
    if (topic_it == topics_.end()) {
        reply(client_id, make_error(req.request_id, ErrorCode::TOPIC_ERROR, "topic not found"));
        return;
    }
    std::optional<ClientMetadata> existing = registry_.get(client_id);
    if (existing && existing->active) {
        reply(client_id, make_error(req.request_id, ErrorCode::TOPIC_ERROR, "duplicate client name"));
        return;
    }
    if (existing && existing->topic != topic_name) {
        auto old_topic_id = topics_.find(existing->topic);
        if (old_topic_id != topics_.end())
            old_topic_id->second->remove_consumer(client_id);
    }
    uint32_t start_offset = 0;
    if (req.has_offset)
        start_offset = req.requested_offset;
    else if (existing && existing->topic == topic_name)
        start_offset = existing->offset;
    
    ClientMetadata meta;
    meta.client_id = client_id;
    meta.topic = topic_name;
    meta.offset = start_offset;
    meta.prefix = prefix;
    meta.ipc_path = client_queue_name(pid_, client_id);
    meta.active = true;
    registry_.upsert(meta);

    ServerToClientMessage ok = make_ok(req.request_id);
    ok.response.offset = start_offset;
    std::strncpy(ok.response.topic_name, topic_name.c_str(), MAX_ID_LEN);
    std::strncpy(ok.response.prefix, prefix.c_str(), MAX_PREFIX_LEN);
    std::strncpy(ok.response.ipc_path, meta.ipc_path.c_str(), MAX_IPC_PATH_LEN - 1);
    reply(client_id, ok);

    topic_it->second->add_consumer(client_id, prefix, meta.ipc_path, start_offset);
}

void Server::handle_produce_start(const IpcRequest& req) {
    std::string client_id = field_to_string(req.client_id, sizeof(req.client_id));
    std::string topic_name = field_to_string(req.topic_name, sizeof(req.topic_name));
    if (!is_valid_id(topic_name)) {
        reply(client_id, make_error(req.request_id, ErrorCode::GENERAL, "invalid topic name"));
        return;
    }
    auto topic_it = topics_.find(topic_name);
    if (topic_it == topics_.end()) {
        reply(client_id, make_error(req.request_id, ErrorCode::TOPIC_ERROR, "topic not found"));
        return;
    }

    ServerToClientMessage ok = make_ok(req.request_id);
    std::strncpy(ok.response.topic_name, topic_name.c_str(), MAX_ID_LEN);
    std::strncpy(ok.response.ipc_path, topic_it->second->data_queue_name().c_str(), MAX_IPC_PATH_LEN - 1);
    reply(client_id, ok);
}

void Server::handle_info(const IpcRequest& req) {
    reply(field_to_string(req.client_id, sizeof(req.client_id)),
          make_error(req.request_id, ErrorCode::GENERAL, "not implemented yet"));
}

void Server::handle_consumer_ack(const IpcRequest& req) {
    reply(field_to_string(req.client_id, sizeof(req.client_id)),
          make_error(req.request_id, ErrorCode::GENERAL, "not implemented yet"));
}

void Server::handle_disconnect(const IpcRequest& req) {
    reply(field_to_string(req.client_id, sizeof(req.client_id)),
          make_error(req.request_id, ErrorCode::GENERAL, "not implemented yet"));
}
}
