#ifndef TREENITY_CLIENT_FIELDS_HPP
#define TREENITY_CLIENT_FIELDS_HPP

#include <cstring>
#include <string>

namespace treenity::client {

// Reads a NUL-terminated (or fully-packed) fixed-size protocol field as a
// std::string, the same way the server does for its own copy of IpcRequest
// fields. Never reads past `capacity`.
inline std::string field_to_string(const char* field, std::size_t capacity) {
    return std::string(field, strnlen(field, capacity));
}

}

#endif // TREENITY_CLIENT_FIELDS_HPP
