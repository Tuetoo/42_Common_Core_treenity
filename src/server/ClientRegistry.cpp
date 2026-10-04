#include "ClientRegistry.hpp"

#include <mutex>

namespace treenity {

bool ClientRegistry::insert(const ClientMetadata& meta) {
    std::unique_lock lock(mutex_);
    return clients_.insert(meta.client_id, meta);
}

void ClientRegistry::upsert(const ClientMetadata& meta) {
    std::unique_lock lock(mutex_);
    clients_.upsert(meta.client_id, meta);
}

bool ClientRegistry::exists(const std::string& client_id) const {
    std::shared_lock lock(mutex_);
    return clients_.find(client_id) != nullptr;
}

std::optional<ClientMetadata> ClientRegistry::get(const std::string& client_id) const {
    std::shared_lock lock(mutex_);
    const ClientMetadata* found = clients_.find(client_id);
    if (!found)
        return std::nullopt;
    return *found;
}

bool ClientRegistry::set_offset(const std::string& client_id, uint32_t new_offset) {
    std::unique_lock lock(mutex_);
    ClientMetadata* found = clients_.find(client_id);
    if (!found)
        return false;
    found->offset = new_offset;
    return true;
}

bool ClientRegistry::set_active(const std::string& client_id, bool active) {
    std::unique_lock lock(mutex_);
    ClientMetadata* found = clients_.find(client_id);
    if (!found)
        return false;
    found->active = active;
    return true;
}

bool ClientRegistry::remove(const std::string& client_id) {
    std::unique_lock lock(mutex_);
    return clients_.erase(client_id);
}
}