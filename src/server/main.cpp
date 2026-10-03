#include "Server.hpp"

#include <exception>
#include <iostream>

int main() {
    try {
        treenity::Server server;
        server.run();
    } catch (const std::exception& e) {
        std::cerr << "server: fatal: " << e.what() << "\n";
        return 1;
    }
    return 0;
}