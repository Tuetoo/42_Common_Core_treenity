#include "IpcClient.hpp"

#include <cerrno>
#include <cstring>
#include <fcntl.h>

namespace treenity::client {

IpcClient::IpcClient(std::string main_queue_path, std::string client_id)
    : main_queue_path_(std::move(main_queue_path)), client_id_(std::move(client_id)) {}

IpcClient::~IpcClient() {
    cleanup();
}

bool IpcClient::connect() {
    own_queue_path_ = main_queue_path_ + "." + client_id_;

    struct mq_attr attr {};
    attr.mq_flags = 0;
    attr.mq_maxmsg = QUEUE_MAX_MSG_COUNT;
    attr.mq_msgsize = CLIENT_QUEUE_MAX_MSG_SIZE;
    attr.mq_curmsgs = 0;

    mq_unlink(own_queue_path_.c_str());
    own_mq_ = mq_open(own_queue_path_.c_str(), O_CREAT | O_RDONLY, 0600, &attr);
    if (own_mq_ == static_cast<mqd_t>(-1))
        return false;

    main_mq_ = mq_open(main_queue_path_.c_str(), O_WRONLY);
    if (main_mq_ == static_cast<mqd_t>(-1)) {
        cleanup();
        return false;
    }
    return true;
}

uint64_t IpcClient::next_request_id() {
    return next_request_id_++;
}

bool IpcClient::request(IpcRequest req, IpcResponse& out) {
    std::strncpy(req.client_id, client_id_.c_str(), MAX_ID_LEN);
    req.request_id = next_request_id();
    if (mq_send(main_mq_, reinterpret_cast<const char*>(&req), sizeof(req), 0) == -1)
        return false;

    char buf[CLIENT_QUEUE_MAX_MSG_SIZE];
    ssize_t n = mq_receive(own_mq_, buf, sizeof(buf), nullptr);
    if (n < 0)
        return false;

    ServerToClientMessage msg{};
    std::memcpy(&msg, buf, sizeof(msg));
    out = msg.response;
    return true;
}

bool IpcClient::send_request(IpcRequest req) {
    std::strncpy(req.client_id, client_id_.c_str(), MAX_ID_LEN);
    req.request_id = next_request_id();
    return mq_send(main_mq_, reinterpret_cast<const char*>(&req), sizeof(req), 0) != -1;
}

RecvResult IpcClient::receive(ServerToClientMessage& out) {
    char buf[CLIENT_QUEUE_MAX_MSG_SIZE];
    ssize_t n = mq_receive(own_mq_, buf, sizeof(buf), nullptr);
    if (n < 0)
        return (errno == EINTR) ? RecvResult::INTERRUPTED : RecvResult::ERROR;
    std::memcpy(&out, buf, sizeof(out));
    return RecvResult::OK;
}

void IpcClient::cleanup() {
    if (own_mq_ != static_cast<mqd_t>(-1)) {
        mq_close(own_mq_);
        mq_unlink(own_queue_path_.c_str());
        own_mq_ = static_cast<mqd_t>(-1);
    }
    if (main_mq_ != static_cast<mqd_t>(-1)) {
        mq_close(main_mq_);
        main_mq_ = static_cast<mqd_t>(-1);
    }
}

}
