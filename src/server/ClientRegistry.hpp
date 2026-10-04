#ifndef TREENITY_CLIENT_REGISTRY_HPP
#define TREENITY_CLIENT_REGISTRY_HPP

#include "HashMap.hpp"

#include <cstdint>
#include <optional>
#include <shared_mutex>
#include <string>

namespace treenity {

struct ClientMetadata {
    std::string client_id;
    std::string topic;
    uint32_t    offset = 0;
    std::string prefix;
    std::string ipc_path;
    bool        active = false;
};

class ClientRegistry {
public:
    bool insert(const ClientMetadata& meta);
    void upsert(const ClientMetadata& meta);
    bool exists(const std::string& client_id) const;
    std::optional<ClientMetadata> get(const std::string& client_id) const;
    bool set_offset(const std::string& client_id, uint32_t new_offset);
    bool set_active(const std::string& client_id, bool active);
    bool remove(const std::string& client_id);

private:
    mutable std::shared_mutex mutex_;
    HashMap<std::string, ClientMetadata> clients_;
};
}

#endif // TREENITY_CLIENT_REGISTRY_HPP