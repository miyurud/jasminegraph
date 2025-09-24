//
// Created by kumarawansha on 1/2/25.
//

#ifndef JASMINEGRAPH_SHAREDBUFFER_H
#define JASMINEGRAPH_SHAREDBUFFER_H

#include <iostream>
#include <deque>
#include <mutex>
#include <condition_variable>
#include <optional>
#include <string>
#include <optional>


class SharedBuffer {
private:
    std::deque<std::string> buffer;
    std::mutex mtx;
    std::condition_variable cv;
    const size_t max_size;

public:
    explicit SharedBuffer(size_t size) : max_size(size) {}

    // Add data to the buffer
    void add(const std::string &data);

    // Retrieve data from the buffer
    std::string get();

    bool tryGet(std::string& data);

    bool empty();
    void clear();
    std::optional<std::string> getWithTimeout(int timeoutSeconds);
};

#endif  // JASMINEGRAPH_SHAREDBUFFER_H
