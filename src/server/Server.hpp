#ifndef TREENITY_SERVER_HPP
#define TREENITY_SERVER_HPP

#include "Topic.hpp"
#include "ipc_protocol.h"
#include "ClientRegistry.hpp"

#include <mqueue.h>
#include <string>
#include <sys/types.h>
#include <map>
#include <memory>

namespace treenity {

class Server {
public:
    Server();
    ~Server();

    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    void run();

private:
    void handle_create_topic(const IpcRequest& req);
    void handle_list_topics(const IpcRequest& req);
    void handle_register_client(const IpcRequest& req);
    void handle_produce_start(const IpcRequest& req);
    void handle_info(const IpcRequest& req);
    void handle_consumer_ack(const IpcRequest& req);
    void handle_disconnect(const IpcRequest& req);
    void reply(const std::string& client_id, const ServerToClientMessage& msg) const;
    static ServerToClientMessage make_ok(uint64_t request_id);
    static ServerToClientMessage make_error(uint64_t request_id, ErrorCode code, const std::string& message);
    static bool is_valid_id(const std::string& id);

    pid_t       pid_;
    std::string main_queue_name_;
    mqd_t       main_mq_ = static_cast<mqd_t>(-1);

    std::map<std::string, std::unique_ptr<Topic>>   topics_;
    ClientRegistry                                  registry_;
};
}
#endif // TREENITY_SERVER_HPP