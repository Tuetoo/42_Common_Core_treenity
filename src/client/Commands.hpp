#ifndef TREENITY_CLIENT_COMMANDS_HPP
#define TREENITY_CLIENT_COMMANDS_HPP

#include <cstdint>
#include <string>

namespace treenity::client {

int run_create(const std::string& ipc_path, const std::string& topic_name);
int run_list(const std::string& ipc_path);
int run_produce(const std::string& ipc_path, const std::string& topic_name, bool raw);

int run_subscribe(const std::string& ipc_path, const std::string& topic_name,
                   const std::string& subscriber_name,
                   bool has_prefix, const std::string& prefix,
                   bool has_offset, uint32_t offset,
                   bool raw);

int run_info(const std::string& ipc_path, const std::string& target_client_id);

}

#endif // TREENITY_CLIENT_COMMANDS_HPP
