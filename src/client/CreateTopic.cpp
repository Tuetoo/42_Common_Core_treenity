#include "Commands.hpp"
#include "Ephemeral.hpp"
#include "Errors.hpp"
#include "IpcClient.hpp"
#include "Validation.hpp"
#include "ipc_protocol.h"

#include <cstring>
#include <iostream>

namespace treenity::client {

int run_create(const std::string& ipc_path, const std::string& topic_name) {
    if (!is_valid_id(topic_name))
        return fail("invalid topic name", 1);

    IpcClient client(ipc_path, ephemeral_client_id('c'));
    if (!client.connect())
        return fail("could not reach server at '" + ipc_path + "'", 1);

    IpcRequest req{};
    req.type = MessageType::CREATE_TOPIC;
    std::strncpy(req.topic_name, topic_name.c_str(), MAX_ID_LEN);

    IpcResponse resp;
    bool ok = client.request(req, resp);
    client.cleanup();
    if (!ok)
        return fail("communication with server failed", 3);
    if (resp.error_code != ErrorCode::NONE)
        return fail(resp.error_message, exit_code_for(resp.error_code));

    std::cout << "topic created" << std::endl;
    return 0;
}

}
