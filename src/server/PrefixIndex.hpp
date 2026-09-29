#ifndef TREENITY_PREFIX_INDEX_HPP
#define TREENITY_PREFIX_INDEX_HPP

#include <string>
#include <vector>

namespace treenity {
struct ConsumerHandle {
    std::string client_id;
    std::string prefix;
    std::string ipc_path;
};

class PrefixIndex {
public:
    void add(const ConsumerHandle& consumer);
    void remove(const std::string& client_id);
    std::vector<ConsumerHandle> match(const std::string& key) const;
    std::vector<ConsumerHandle> all() const;

private:
    std::vector<ConsumerHandle> consumers_;
};
}

#endif // TREENITY_PREFIX_INDEX_HPP