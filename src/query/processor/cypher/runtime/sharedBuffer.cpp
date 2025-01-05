//
// Created by kumarawansha on 1/2/25.
//

#include "sharedBuffer.h"

// Add data to the buffer
void SharedBuffer::add(const std::string &data) {
    std::unique_lock<std::mutex> lock(mtx);
    cv.wait(lock, [this]() { return buffer.size() < max_size; });

    buffer.push_back(data);
    std::cout << "Added: " << data << std::endl;

    cv.notify_one(); // Notify waiting threads
}

// Retrieve data from the buffer
std::string SharedBuffer::get() {
    std::unique_lock<std::mutex> lock(mtx);
    cv.wait(lock, [this]() { return !buffer.empty(); });

    std::string data = buffer.front();
    buffer.pop_front();
    std::cout << "Retrieved: " << data << std::endl;

    cv.notify_one(); // Notify waiting threads
    return data;
}