#include "ClientRegistry.hpp"
#include <mutex>

namespace treenity {

bool ClientRegistry::insert(const ClientMetadata& meta) {
    std::unique_lock lock(mutex_);
    auto [it, inserted] = clients_.emplace(meta.client_id, meta);
    (void)it;
    return inserted;
}

void ClientRegistry::upsert(const ClientMetadata& meta) {
    std::unique_lock lock(mutex_);
    clients_[meta.client_id] = meta;
}

bool ClientRegistry::exists(const std::string& client_id) const {
    std::shared_lock lock(mutex_);
    return clients_.find(client_id) != clients_.end();
}

std::optional<ClientMetadata> ClientRegistry::get(const std::string& client_id) const {
    std::shared_lock lock(mutex_);
    auto it = clients_.find(client_id);
    if (it == clients_.end())
        return std::nullopt;
    return it->second;
}

bool ClientRegistry::set_offset(const std::string& client_id, uint32_t new_offset) {
    std::unique_lock lock(mutex_);
    auto it = clients_.find(client_id);
    if (it == clients_.end())
        return false;
    it->second.offset = new_offset;
    return true;
}

bool ClientRegistry::set_active(const std::string& client_id, bool active) {
    std::unique_lock lock(mutex_);
    auto it = clients_.find(client_id);
    if (it == clients_.end())
        return false;
    it->second.active = active;
    return true;
}

bool ClientRegistry::remove(const std::string& client_id) {
    std::unique_lock lock(mutex_);
    return clients_.erase(client_id) > 0;
}
}