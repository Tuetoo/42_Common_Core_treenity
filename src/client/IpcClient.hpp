#ifndef TREENITY_CLIENT_IPC_CLIENT_HPP
#define TREENITY_CLIENT_IPC_CLIENT_HPP

#include "ipc_protocol.h"

#include <cstdint>
#include <mqueue.h>
#include <string>

namespace treenity::client {

enum class RecvResult { OK, INTERRUPTED, ERROR };

// Owns one client's side of the protocol: this process's dedicated reply
// queue, and the write end of the server's main queue. One instance per
// client_id in use for the lifetime of a single command (a fresh ephemeral
// id for one-shot commands, the subscriber name for `subscribe`).
class IpcClient {
public:
    IpcClient(std::string main_queue_path, std::string client_id);
    ~IpcClient();

    IpcClient(const IpcClient&) = delete;
    IpcClient& operator=(const IpcClient&) = delete;

    // Creates this client's own reply queue, then opens the server's main
    // queue for writing. False means "invalid IPC identifier" (exit 1):
    // either queue could not be opened.
    bool connect();

    // Sends `req` over the main queue (client_id/request_id filled in
    // here) and blocks for exactly one reply on this client's own queue.
    // False means an IPC-level failure (exit 3).
    bool request(IpcRequest req, IpcResponse& out);

    // Fire-and-forget send (CONSUMER_ACK, DISCONNECT): no reply is
    // awaited. False means an IPC-level failure.
    bool send_request(IpcRequest req);

    // Blocks for the next message on this client's own queue (used by
    // `subscribe` to receive CONSUMER_MESSAGE / SHUTDOWN deliveries).
    RecvResult receive(ServerToClientMessage& out);

    const std::string& client_id() const { return client_id_; }

    // Closes and mq_unlink()s this client's own queue. Idempotent.
    void cleanup();

private:
    uint64_t next_request_id();

    std::string main_queue_path_;
    std::string client_id_;
    std::string own_queue_path_;
    mqd_t       main_mq_ = static_cast<mqd_t>(-1);
    mqd_t       own_mq_  = static_cast<mqd_t>(-1);
    uint64_t    next_request_id_ = 1;
};

}

#endif // TREENITY_CLIENT_IPC_CLIENT_HPP
