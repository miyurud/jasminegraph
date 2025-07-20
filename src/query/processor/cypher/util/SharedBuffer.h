//
// SharedBuffer.h - Lock-free implementation with same API
//

#ifndef JASMINEGRAPH_SHAREDBUFFER_H
#define JASMINEGRAPH_SHAREDBUFFER_H

#include <string>
#include <vector>
#include <atomic>
#include <thread>
#include <chrono>

class SharedBuffer {
private:
    std::vector<std::string> buffer;
    const size_t max_size;
    std::atomic<size_t> head{0};
    std::atomic<size_t> tail{0};
    std::atomic<bool> closed{false};

    inline size_t next(size_t idx) const { return (idx + 1) % max_size; }

public:
    explicit SharedBuffer(size_t size) : buffer(size), max_size(size) {}

    // Add data (blocking if full)
    void add(const std::string &data);

    // Retrieve data (blocking if empty)
    std::string get();

    // Non-blocking get
    bool tryGet(std::string &data);

    bool empty();

    void close();
};

#endif  // JASMINEGRAPH_SHAREDBUFFER_H
