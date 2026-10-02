#ifndef TREENITY_SERVER_HPP
#define TREENITY_SERVER_HPP

#include "ipc_protocol.h"

#include <mqueue.h>
#include <string>
#include <sys/types.h>

namespace treenity {

class Server {
public:
    Server();
    ~Server();

    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    void run();

private:
    pid_t       pid_;
    std::string main_queue_name_;
    mqd_t       main_mq_ = static_cast<mqd_t>(-1);
};
}
#endif // TREENITY_SERVER_HPP