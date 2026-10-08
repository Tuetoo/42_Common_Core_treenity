#include "Commands.hpp"
#include "Errors.hpp"

#include <cctype>
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

namespace {

void print_usage() {
    std::cerr <<
        "usage:\n"
        "  client <ipc_identifier> create <topic_name>\n"
        "  client <ipc_identifier> list\n"
        "  client <ipc_identifier> produce <topic_name> [--raw]\n"
        "  client <ipc_identifier> subscribe <topic_name> <subscriber_name> "
        "[--prefix <prefix>] [--offset <offset>] [--raw]\n"
        "  client <ipc_identifier> info <subscriber_name>\n";
}

bool parse_offset(const std::string& text, uint32_t& out) {
    if (text.empty() || text.size() > 10)
        return false;
    uint64_t value = 0;
    for (unsigned char c : text) {
        if (!std::isdigit(c))
            return false;
        value = value * 10 + (c - '0');
    }
    if (value > UINT32_MAX)
        return false;
    out = static_cast<uint32_t>(value);
    return true;
}

}

int main(int argc, char** argv) {
    using namespace treenity::client;

    if (argc < 3) {
        print_usage();
        return 1;
    }

    std::string ipc_path = argv[1];
    std::string command = argv[2];
    std::vector<std::string> args(argv + 3, argv + argc);

    if (command == "create") {
        if (args.size() != 1) {
            print_usage();
            return 1;
        }
        return run_create(ipc_path, args[0]);
    }

    if (command == "list") {
        if (!args.empty()) {
            print_usage();
            return 1;
        }
        return run_list(ipc_path);
    }

    if (command == "produce") {
        if (args.empty() || args.size() > 2) {
            print_usage();
            return 1;
        }
        bool raw = false;
        for (std::size_t i = 1; i < args.size(); ++i) {
            if (args[i] != "--raw") {
                print_usage();
                return 1;
            }
            raw = true;
        }
        return run_produce(ipc_path, args[0], raw);
    }

    if (command == "subscribe") {
        if (args.size() < 2) {
            print_usage();
            return 1;
        }
        const std::string& topic = args[0];
        const std::string& subscriber = args[1];
        bool raw = false;
        bool has_prefix = false;
        std::string prefix;
        bool has_offset = false;
        uint32_t offset = 0;

        for (std::size_t i = 2; i < args.size(); ++i) {
            if (args[i] == "--raw") {
                raw = true;
            } else if (args[i] == "--prefix") {
                if (i + 1 >= args.size()) {
                    print_usage();
                    return 1;
                }
                has_prefix = true;
                prefix = args[++i];
            } else if (args[i] == "--offset") {
                if (i + 1 >= args.size()) {
                    print_usage();
                    return 1;
                }
                if (!parse_offset(args[++i], offset))
                    return fail("invalid --offset value", 1);
                has_offset = true;
            } else {
                print_usage();
                return 1;
            }
        }
        return run_subscribe(ipc_path, topic, subscriber, has_prefix, prefix, has_offset, offset, raw);
    }

    if (command == "info") {
        if (args.size() != 1) {
            print_usage();
            return 1;
        }
        return run_info(ipc_path, args[0]);
    }

    print_usage();
    return 1;
}
