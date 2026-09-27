#ifndef TREENITY_IPC_PROTOCOL_H
#define TREENITY_IPC_PROTOCOL_H

#include <cstdint>
#include <cstddef>
#include <string>
#include <sys/types.h>

namespace treenity {
    constexpr std::size_t MAX_ID_LEN           = 32;
    constexpr std::size_t MAX_PREFIX_LEN       = 32;
    constexpr std::size_t MAX_KV_LEN           = 1024;
    constexpr std::size_t MAX_IPC_PATH_LEN     = 128;
    constexpr std::size_t MAX_ERROR_MSG_LEN    = 128;
    constexpr std::size_t MAX_TOPIC_LIST_LEN   = 512;
    constexpr long MAIN_QUEUE_MAX_MSG_SIZE     = 300;
    constexpr long CLIENT_QUEUE_MAX_MSG_SIZE   = 1200;
    constexpr long QUEUE_MAX_MSG_COUNT         = 10;
    constexpr long PRODUCE_QUEUE_MAX_MSG_SIZE  = 1100;

    enum class MessageType : uint8_t {
        // Client -> Server
        CREATE_TOPIC        = 1,
        LIST_TOPICS         = 2,
        REGISTER_CLIENT     = 3,
        PRODUCE_START       = 4,
        INFO                = 5,
        CONSUMER_ACK        = 6,
        DISCONNECT          = 7,

        // Server -> Client, response to a request
        RESPONSE_OK         = 100,
        RESPONSE_ERROR      = 101,

        // Server -> Client, async delivery
        CONSUMER_MESSAGE    = 150,
        SHUTDOWN            = 151,
    };

    enum class ErrorCode : uint8_t {
        NONE        = 0,
        GENERAL     = 1,
        TOPIC_ERROR = 2,
        IPC_ERROR   = 3,
    };

    struct IpcRequest {
        MessageType type;
        uint64_t    request_id;
        char        client_id[MAX_ID_LEN + 1]       = {0};
        char        topic_name[MAX_ID_LEN + 1]      = {0};
        char        prefix[MAX_PREFIX_LEN + 1]      = {0};
        char        query_client_id[MAX_ID_LEN + 1] = {0};
        bool        has_offset                      = false;
        uint32_t    requested_offset                = 0;
        uint32_t    ack_offset                      = 0;
    };

    struct IpcResponse {
        MessageType type;
        uint64_t    request_id;
        ErrorCode   error_code                          = ErrorCode::NONE;
        char        error_message[MAX_ERROR_MSG_LEN]    = {0};
        char        topic_list[MAX_TOPIC_LIST_LEN]      = {0};
        char        client_id[MAX_ID_LEN + 1]           = {0};
        char        topic_name[MAX_ID_LEN + 1]          = {0};
        char        prefix[MAX_PREFIX_LEN + 1]          = {0};
        char        ipc_path[MAX_IPC_PATH_LEN]          = {0};
        uint32_t    offset                              = 0;
    };

    struct ConsumerMessage {
        MessageType type;
        uint32_t    offset              = 0;
        uint32_t    key_size            = 0;
        uint32_t    value_size          = 0;
        char        data[MAX_KV_LEN]    = {0};
    };

    struct ServerToClientMessage {
        MessageType type;
        union {
            IpcResponse     response;
            ConsumerMessage consumer_msg;
        };
    };

    struct ProduceRecord {
        uint32_t    key_size            = 0;
        uint32_t    value_size          = 0;
        char        data[MAX_KV_LEN]    = {0};
    };

    inline std::string main_queue_name(pid_t server_pid) {
        return "/treenity.server." + std::to_string(server_pid);
    }

    inline std::string client_queue_name(pid_t server_pid, const std::string& client_id) {
        return "/treenity.server." + std::to_string(server_pid) + "." + client_id;
    }

    inline std::string topic_data_queue_name(pid_t server_pid, const std::string& topic_name) {
        return "/treenity.server." + std::to_string(server_pid) + ".topic." + topic_name;
    }

    static_assert(sizeof(IpcRequest) <= static_cast<std::size_t>(MAIN_QUEUE_MAX_MSG_SIZE),
                    "IpcRequest exceeds MAIN_QUEUE_MAX_MSG_SIZE");
    static_assert(sizeof(ServerToClientMessage) <= static_cast<std::size_t>(CLIENT_QUEUE_MAX_MSG_SIZE),
                    "ServerToClientMessage exceeds CLIENT_QUEUE_MAX_MSG_SIZE");
    static_assert(sizeof(ProduceRecord) <= static_cast<std::size_t>(PRODUCE_QUEUE_MAX_MSG_SIZE),
                    "ProduceRecord exceeds PRODUCE_QUEUE_MAX_MSG_SIZE");
}

#endif