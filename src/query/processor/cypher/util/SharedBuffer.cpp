//
// Created by kumarawansha on 1/2/25.
//

#include "SharedBuffer.h"

// Add data to the buffer
void SharedBuffer::add(const std::string &data) {
    std::unique_lock<std::mutex> lock(mtx);
    cv.wait(lock, [this]() { return buffer.size() < max_size; });
    buffer.push_back(data);
    lock.unlock();
    cv.notify_one();  // Notify waiting threads
}

// Retrieve data from the buffer
std::string SharedBuffer::get() {
    std::unique_lock<std::mutex> lock(mtx);
    cv.wait(lock, [this]() { return !buffer.empty(); });
    std::string data = buffer.front();
    buffer.pop_front();
    lock.unlock();
    cv.notify_one();  // Notify waiting threads
    return data;
}

// Non-blocking method to try getting data
bool SharedBuffer::tryGet(std::string& data) {
    std::unique_lock<std::mutex> lock(mtx);
    if (buffer.empty()) {
        return false;  // No data available
    }
    data = buffer.front();
    buffer.pop_front();
    cv.notify_one();  // Notify waiting threads
    return true;
}

bool SharedBuffer::empty() {
    std::lock_guard<std::mutex> lock(mtx);
    return buffer.empty();
}


