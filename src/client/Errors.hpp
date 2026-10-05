#ifndef TREENITY_CLIENT_ERRORS_HPP
#define TREENITY_CLIENT_ERRORS_HPP

#include "ipc_protocol.h"

#include <iostream>
#include <string>

namespace treenity::client {

// ErrorCode values already line up with the subject's exit codes
// (NONE=0, GENERAL=1, TOPIC_ERROR=2, IPC_ERROR=3), so forwarding a
// server-reported error is a direct cast.
inline int exit_code_for(ErrorCode code) {
    return static_cast<int>(code);
}

// Writes `message` to stderr and returns `code`, to be used directly as
// the command's return value: `return fail("invalid topic name", 1);`
inline int fail(const std::string& message, int code) {
    std::cerr << "client: " << message << "\n";
    return code;
}

}

#endif // TREENITY_CLIENT_ERRORS_HPP
