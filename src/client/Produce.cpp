#include "Commands.hpp"
#include "Codec.hpp"
#include "Ephemeral.hpp"
#include "Errors.hpp"
#include "Fields.hpp"
#include "IpcClient.hpp"
#include "Validation.hpp"
#include "ipc_protocol.h"

#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <mqueue.h>
#include <string>

namespace treenity::client {
namespace {

int send_record(mqd_t data_mq, const std::string& key, const std::string& value) {
    if (key.size() + value.size() > MAX_KV_LEN)
        return fail("record exceeds the " + std::to_string(MAX_KV_LEN) + "-byte key+body limit", 1);

    ProduceRecord rec{};
    rec.key_size = static_cast<uint32_t>(key.size());
    rec.value_size = static_cast<uint32_t>(value.size());
    std::memcpy(rec.data, key.data(), key.size());
    std::memcpy(rec.data + key.size(), value.data(), value.size());

    if (mq_send(data_mq, reinterpret_cast<const char*>(&rec), sizeof(rec), 0) == -1)
        return fail("failed to send record to topic queue", 3);
    return 0;
}

int produce_text(mqd_t data_mq) {
    std::string line;
    while (std::getline(std::cin, line)) {
        std::string key, value;
        if (!split_text_line(line, key, value))
            return fail("malformed input line (expected 'key:body'): " + line, 1);
        int rc = send_record(data_mq, key, value);
        if (rc != 0)
            return rc;
    }
    return 0;
}

int produce_raw(mqd_t data_mq) {
    while (true) {
        char size_buf[4];
        ReadStatus status = read_exact(std::cin, size_buf, sizeof(size_buf));
        if (status == ReadStatus::EOF_CLEAN)
            return 0;
        if (status == ReadStatus::EOF_PARTIAL)
            return fail("partial record at EOF (truncated key size)", 1);
        uint32_t key_size = read_u32_le(size_buf);

        std::string key(key_size, '\0');
        if (key_size > 0 && read_exact(std::cin, key.data(), key_size) != ReadStatus::OK)
            return fail("partial record at EOF (truncated key)", 1);

        status = read_exact(std::cin, size_buf, sizeof(size_buf));
        if (status != ReadStatus::OK)
            return fail("partial record at EOF (truncated value size)", 1);
        uint32_t value_size = read_u32_le(size_buf);

        std::string value(value_size, '\0');
        if (value_size > 0 && read_exact(std::cin, value.data(), value_size) != ReadStatus::OK)
            return fail("partial record at EOF (truncated value)", 1);

        int rc = send_record(data_mq, key, value);
        if (rc != 0)
            return rc;
    }
}

}

int run_produce(const std::string& ipc_path, const std::string& topic_name, bool raw) {
    if (!is_valid_id(topic_name))
        return fail("invalid topic name", 1);

    IpcClient client(ipc_path, ephemeral_client_id('p'));
    if (!client.connect())
        return fail("could not reach server at '" + ipc_path + "'", 1);

    IpcRequest req{};
    req.type = MessageType::PRODUCE_START;
    std::strncpy(req.topic_name, topic_name.c_str(), MAX_ID_LEN);

    IpcResponse resp;
    bool ok = client.request(req, resp);
    if (!ok) {
        client.cleanup();
        return fail("communication with server failed", 3);
    }
    if (resp.error_code != ErrorCode::NONE) {
        client.cleanup();
        return fail(resp.error_message, exit_code_for(resp.error_code));
    }
    std::string data_queue_path = field_to_string(resp.ipc_path, sizeof(resp.ipc_path));
    client.cleanup();

    mqd_t data_mq = mq_open(data_queue_path.c_str(), O_WRONLY);
    if (data_mq == static_cast<mqd_t>(-1))
        return fail("could not open topic data queue '" + data_queue_path + "'", 3);

    int rc = raw ? produce_raw(data_mq) : produce_text(data_mq);
    mq_close(data_mq);
    return rc;
}

}
