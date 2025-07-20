#include "SharedBuffer.h"

void SharedBuffer::add(const std::string &data) {
    size_t t;
    size_t next_t;
    while (true) {
        t = tail.load(std::memory_order_relaxed);
        next_t = next(t);
        if (next_t == head.load(std::memory_order_acquire)) {
            // Buffer full, wait
            if (closed.load()) return;
            std::this_thread::sleep_for(std::chrono::microseconds(10));
            continue;
        }
        buffer[t] = data;
        tail.store(next_t, std::memory_order_release);
        return;
    }
}

std::string SharedBuffer::get() {
    std::string result;
    while (!tryGet(result)) {
        if (closed.load() && empty()) return "-1"; // Sentinel for EOF
        std::this_thread::sleep_for(std::chrono::microseconds(10));
    }
    return result;
}

bool SharedBuffer::tryGet(std::string &data) {
    size_t h = head.load(std::memory_order_relaxed);
    if (h == tail.load(std::memory_order_acquire)) return false; // empty
    data = std::move(buffer[h]);
    head.store(next(h), std::memory_order_release);
    return true;
}

bool SharedBuffer::empty() {
    return head.load(std::memory_order_acquire) ==
           tail.load(std::memory_order_acquire);
}

void SharedBuffer::close() {
    closed.store(true, std::memory_order_release);
}
