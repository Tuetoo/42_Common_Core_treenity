#ifndef TREENITY_CLIENT_VALIDATION_HPP
#define TREENITY_CLIENT_VALIDATION_HPP

#include "ipc_protocol.h"

#include <cctype>
#include <string>

namespace treenity::client {

// Mirrors Server::is_valid_id exactly: ^[a-zA-Z0-9_.-]{1,32}$.
// Client ids and topic names MUST be checked with this before any request
// is sent. The server silently drops a request whose client_id fails this
// check (it has no reply queue to answer on), so skipping this check here
// would make the client hang forever waiting for a reply that never comes.
inline bool is_valid_id(const std::string& id) {
    if (id.empty() || id.size() > MAX_ID_LEN)
        return false;
    for (unsigned char c : id) {
        if (!std::isalnum(c) && c != '_' && c != '.' && c != '-')
            return false;
    }
    return true;
}

}

#endif // TREENITY_CLIENT_VALIDATION_HPP
