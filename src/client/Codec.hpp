#ifndef TREENITY_CLIENT_CODEC_HPP
#define TREENITY_CLIENT_CODEC_HPP

#include <cstdint>
#include <cstddef>
#include <istream>
#include <ostream>
#include <string>

namespace treenity::client {

// Explicit little-endian encode/decode, independent of host byte order
// (the subject requires LE on the wire regardless of platform).
void write_u32_le(std::ostream& out, uint32_t value);
uint32_t read_u32_le(const char bytes[4]);

enum class ReadStatus { OK, EOF_CLEAN, EOF_PARTIAL };

// Reads exactly `n` bytes into `dst`. EOF_CLEAN means zero bytes were
// available (a legal place to stop); EOF_PARTIAL means the stream ended
// mid-field, which is a producer error (subject: exit code 1).
ReadStatus read_exact(std::istream& in, char* dst, std::size_t n);

// Splits a text-mode producer line "key:body" at the first ':'.
// Returns false if there is no ':' in the line at all.
bool split_text_line(const std::string& line, std::string& key, std::string& body);

}

#endif // TREENITY_CLIENT_CODEC_HPP
