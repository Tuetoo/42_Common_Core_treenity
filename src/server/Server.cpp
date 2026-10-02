#include "Server.hpp"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <stdexcept>
#include <unistd.h>

namespace treenity {

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
}
}
