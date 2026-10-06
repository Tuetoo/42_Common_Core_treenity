#include "Commands.hpp"
#include "Ephemeral.hpp"
#include "Errors.hpp"
#include "Fields.hpp"
#include "IpcClient.hpp"
#include "ipc_protocol.h"

#include <iostream>

namespace treenity::client {

int run_list(const std::string& ipc_path) {
    IpcClient client(ipc_path, ephemeral_client_id('l'));
    if (!client.connect())
        return fail("could not reach server at '" + ipc_path + "'", 1);

    IpcRequest req{};
    req.type = MessageType::LIST_TOPICS;

    IpcResponse resp;
    bool ok = client.request(req, resp);
    client.cleanup();
    if (!ok)
        return fail("communication with server failed", 3);
    if (resp.error_code != ErrorCode::NONE)
        return fail(resp.error_message, exit_code_for(resp.error_code));

    std::cout << field_to_string(resp.topic_list, sizeof(resp.topic_list)) << std::endl;
    return 0;
}

}
