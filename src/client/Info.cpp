#include "Commands.hpp"
#include "Ephemeral.hpp"
#include "Errors.hpp"
#include "Fields.hpp"
#include "IpcClient.hpp"
#include "Validation.hpp"
#include "ipc_protocol.h"

#include <cstring>
#include <iostream>

namespace treenity::client {
namespace {

std::string json_escape(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    for (char c : s) {
        if (c == '"' || c == '\\')
            out += '\\';
        out += c;
    }
    return out;
}

}

int run_info(const std::string& ipc_path, const std::string& target_client_id) {
    if (!is_valid_id(target_client_id))
        return fail("invalid client name", 1);

    IpcClient client(ipc_path, ephemeral_client_id('i'));
    if (!client.connect())
        return fail("could not reach server at '" + ipc_path + "'", 1);

    IpcRequest req{};
    req.type = MessageType::INFO;
    std::strncpy(req.query_client_id, target_client_id.c_str(), MAX_ID_LEN);

    IpcResponse resp;
    bool ok = client.request(req, resp);
    client.cleanup();
    if (!ok)
        return fail("communication with server failed", 3);
    if (resp.error_code != ErrorCode::NONE)
        return fail(resp.error_message, exit_code_for(resp.error_code));

    std::cout << "{\"client\":\"" << json_escape(field_to_string(resp.client_id, sizeof(resp.client_id))) << "\","
               << "\"topic\":\"" << json_escape(field_to_string(resp.topic_name, sizeof(resp.topic_name))) << "\","
               << "\"offset\":" << resp.offset << ","
               << "\"prefix\":\"" << json_escape(field_to_string(resp.prefix, sizeof(resp.prefix))) << "\","
               << "\"ipc\":\"" << json_escape(field_to_string(resp.ipc_path, sizeof(resp.ipc_path))) << "\"}"
               << std::endl;
    return 0;
}

}
