#include "Commands.hpp"
#include "Codec.hpp"
#include "Errors.hpp"
#include "IpcClient.hpp"
#include "Validation.hpp"
#include "ipc_protocol.h"

#include <csignal>
#include <cstring>
#include <iostream>
#include <string>

namespace treenity::client {
namespace {

volatile sig_atomic_t g_stop_requested = 0;

void on_stop_signal(int) {
    g_stop_requested = 1;
}

// No SA_RESTART: mq_receive() must return EINTR so the receive loop can
// notice the flag instead of blocking forever.
void install_signal_handlers() {
    struct sigaction sa {};
    sa.sa_handler = on_stop_signal;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
}

// Explicit flush after every message: stdout is fully buffered once it's
// not a terminal (e.g. piped into ft_aquarium), and this is meant to be
// consumed in real time as each message arrives, not whenever the libc
// buffer happens to fill up.
void print_text(const ConsumerMessage& msg) {
    std::string key(msg.data, msg.key_size);
    std::string value(msg.data + msg.key_size, msg.value_size);
    std::cout << key << ":" << value << "\n";
    std::cout.flush();
}

void print_raw(const ConsumerMessage& msg) {
    write_u32_le(std::cout, msg.offset);
    write_u32_le(std::cout, msg.key_size);
    std::cout.write(msg.data, msg.key_size);
    write_u32_le(std::cout, msg.value_size);
    std::cout.write(msg.data + msg.key_size, msg.value_size);
    std::cout.flush();
}

}

int run_subscribe(const std::string& ipc_path, const std::string& topic_name,
                   const std::string& subscriber_name,
                   bool has_prefix, const std::string& prefix,
                   bool has_offset, uint32_t offset,
                   bool raw) {
    if (!is_valid_id(subscriber_name))
        return fail("invalid client name", 1);
    if (!is_valid_id(topic_name))
        return fail("invalid topic name", 1);
    if (has_prefix && prefix.size() > MAX_PREFIX_LEN)
        return fail("prefix exceeds " + std::to_string(MAX_PREFIX_LEN) + " characters", 1);

    // The subscriber name IS the protocol client_id: it must stay stable
    // across reconnections so the server can match it against the stored
    // offset (VI.6/VIII.1 "a returning subscriber ... resumes").
    IpcClient client(ipc_path, subscriber_name);
    if (!client.connect())
        return fail("could not reach server at '" + ipc_path + "'", 1);

    install_signal_handlers();

    IpcRequest req{};
    req.type = MessageType::REGISTER_CLIENT;
    std::strncpy(req.topic_name, topic_name.c_str(), MAX_ID_LEN);
    if (has_prefix)
        std::strncpy(req.prefix, prefix.c_str(), MAX_PREFIX_LEN);
    req.has_offset = has_offset;
    req.requested_offset = offset;

    IpcResponse resp;
    if (!client.request(req, resp)) {
        client.cleanup();
        return fail("communication with server failed", 3);
    }
    if (resp.error_code != ErrorCode::NONE) {
        client.cleanup();
        return fail(resp.error_message, exit_code_for(resp.error_code));
    }

    // The server always sends this RESPONSE_OK before any CONSUMER_MESSAGE
    // for this subscription, so printing it here is always correctly
    // ordered before the first delivered message.
    std::cout << "subscribed to " << topic_name << std::endl;

    int exit_code = 0;
    while (!g_stop_requested) {
        ServerToClientMessage msg{};
        RecvResult result = client.receive(msg);
        if (result == RecvResult::INTERRUPTED)
            break; // SIGINT/SIGTERM: fall through to DISCONNECT + exit 0
        if (result == RecvResult::ERROR) {
            exit_code = fail("communication with server failed", 3);
            break;
        }
        if (msg.type == MessageType::SHUTDOWN)
            break; // server sentinel: fall through to DISCONNECT + exit 0
        if (msg.type != MessageType::CONSUMER_MESSAGE)
            continue;

        if (raw)
            print_raw(msg.consumer_msg);
        else
            print_text(msg.consumer_msg);

        IpcRequest ack{};
        ack.type = MessageType::CONSUMER_ACK;
        ack.ack_offset = msg.consumer_msg.offset + 1;
        client.send_request(ack);
    }

    if (exit_code == 0) {
        IpcRequest disconnect{};
        disconnect.type = MessageType::DISCONNECT;
        client.send_request(disconnect); // best-effort, do not wait for the reply
    }
    client.cleanup();
    return exit_code;
}

}
