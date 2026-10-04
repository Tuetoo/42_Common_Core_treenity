#ifndef TREENITY_HASH_MAP_HPP
#define TREENITY_HASH_MAP_HPP

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace treenity {

struct Fnv1aHash {
    std::size_t operator()(const std::string& key) const {
        std::uint64_t hash = 14695981039346656037ULL;
        for (unsigned char c : key) {
            hash ^= c;
            hash *= 1099511628211ULL;
        }
        return static_cast<std::size_t>(hash);
    }
};

template <typename Key, typename Value, typename Hash = Fnv1aHash>
class HashMap {
public:
    explicit HashMap(std::size_t initial_buckets = 16)
        : buckets_(initial_buckets == 0 ? 1 : initial_buckets) {}
    
    HashMap(const HashMap&) = delete;
    HashMap& operator=(const HashMap&) = delete;

    bool insert(const Key& key, const Value& value) {
        if (find(key))
            return false;
        add_node(key, value);
        return true;
    }

    void upsert(const Key& key, const Value& value) {
        if (Value* existing = find(key)) {
            *existing = value;
            return;
        }
        add_node(key, value);
    }

    Value* find(const Key& key) {
        for (Node* node = buckets_[index_of(key)].get(); node; node = node->next.get()) {
            if (node->key == key)
                return &node->value;
        }
        return nullptr;
    }

    const Value* find(const Key& key) const {
        for (const Node* node = buckets_[index_of(key)].get(); node; node = node->next.get()) {
            if (node->key == key)
                return &node->value;
        }
        return nullptr;
    }

    bool erase(const Key& key) {
        std::unique_ptr<Node>* link = &buckets_[index_of(key)];
        while (*link) {
            if ((*link)->key == key) {
                std::unique_ptr<Node> removed = std::move(*link);
                *link = std::move(removed->next);
                --size_;
                return true;
            }
            link = &(*link)->next;
        }
        return false;
    }

    std::size_t size() const { return size_; }
    std::size_t bucket_count() const { return buckets_.size(); }

private:
    struct Node {
        Key key;
        Value value;
        std::unique_ptr<Node> next;
        Node(const Key& k, const Value& v) : key(k), value(v) {}
    };

    static constexpr double MAX_LOAD_FACTOR = 0.75;
    std::size_t index_of(const Key& key) const { return hash_(key) % buckets_.size(); }

    void add_node(const Key& key, const Value& value) {
        if (static_cast<double>(size_ + 1) / static_cast<double>(buckets_.size()) > MAX_LOAD_FACTOR)
            rehash(buckets_.size() * 2);
        auto node = std::make_unique<Node>(key, value);
        std::unique_ptr<Node>& head = buckets_[index_of(key)];
        node->next = std::move(head);
        head = std::move(node);
        ++size_;
    }

    void rehash(std::size_t new_bucket_count) {
        std::vector<std::unique_ptr<Node>> new_buckets(new_bucket_count);
        for (auto& head : buckets_) {
            while (head) {
                std::unique_ptr<Node> node = std::move(head);
                head = std::move(node->next);
                std::size_t index = hash_(node->key) % new_bucket_count;
                node->next = std::move(new_buckets[index]);
                new_buckets[index] = std::move(node);
            }
        }
        buckets_ = std::move(new_buckets);
    }
    std::vector<std::unique_ptr<Node>> buckets_;
    std::size_t size_ = 0;
    Hash hash_;
};
}

#endif // TREENITY_HASH_MAP_HPP