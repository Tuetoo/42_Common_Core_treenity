#ifndef TREENITY_CLIENT_EPHEMERAL_HPP
#define TREENITY_CLIENT_EPHEMERAL_HPP

#include <string>
#include <unistd.h>

namespace treenity::client {

// create/list/produce/info have no explicit client id in the CLI spec but
// still need one internally to route the server's reply. `tag` is a
// single letter per command so concurrent invocations never collide
// (different pids) and the result always matches ^[a-zA-Z0-9_.-]{1,32}$.
inline std::string ephemeral_client_id(char tag) {
    return std::string(1, tag) + std::to_string(getpid());
}

}

#endif // TREENITY_CLIENT_EPHEMERAL_HPP
