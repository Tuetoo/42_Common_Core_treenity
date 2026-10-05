#include "Codec.hpp"

namespace treenity::client {

void write_u32_le(std::ostream& out, uint32_t value) {
    char bytes[4];
    bytes[0] = static_cast<char>(value & 0xFF);
    bytes[1] = static_cast<char>((value >> 8) & 0xFF);
    bytes[2] = static_cast<char>((value >> 16) & 0xFF);
    bytes[3] = static_cast<char>((value >> 24) & 0xFF);
    out.write(bytes, sizeof(bytes));
}

uint32_t read_u32_le(const char bytes[4]) {
    return (static_cast<uint32_t>(static_cast<unsigned char>(bytes[0])))
         | (static_cast<uint32_t>(static_cast<unsigned char>(bytes[1])) << 8)
         | (static_cast<uint32_t>(static_cast<unsigned char>(bytes[2])) << 16)
         | (static_cast<uint32_t>(static_cast<unsigned char>(bytes[3])) << 24);
}

ReadStatus read_exact(std::istream& in, char* dst, std::size_t n) {
    if (n == 0)
        return ReadStatus::OK;
    in.read(dst, static_cast<std::streamsize>(n));
    std::streamsize got = in.gcount();
    if (got == static_cast<std::streamsize>(n))
        return ReadStatus::OK;
    if (got == 0)
        return ReadStatus::EOF_CLEAN;
    return ReadStatus::EOF_PARTIAL;
}

bool split_text_line(const std::string& line, std::string& key, std::string& body) {
    std::size_t sep = line.find(':');
    if (sep == std::string::npos)
        return false;
    key = line.substr(0, sep);
    body = line.substr(sep + 1);
    return true;
}

}
