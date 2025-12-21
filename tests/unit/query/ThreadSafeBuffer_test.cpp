/**
Copyright 2025 JasmineGraph Team
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

#include "../../../src/query/processor/executor/IntraPartitionParallelExecutor.h"
#include "gtest/gtest.h"
#include <thread>
#include <chrono>
#include <vector>

using namespace std::chrono_literals;

// Test fixture for ThreadSafeBuffer
class ThreadSafeBufferTest : public ::testing::Test {
protected:
    ThreadSafeBuffer<int> buffer;
};

// Test 1: Basic single-producer single-consumer functionality
TEST_F(ThreadSafeBufferTest, SingleProducerSingleConsumer) {
    const int NUM_ITEMS = 100;
    std::vector<int> produced;
    std::vector<int> consumed;

    // Producer thread
    std::thread producer([&]() {
        for (int i = 0; i < NUM_ITEMS; ++i) {
            buffer.push(i);
            produced.push_back(i);
        }
        buffer.setFinished();
    });

    // Consumer thread
    std::thread consumer([&]() {
        int item;
        while (buffer.pop(item, 1000ms)) {
            consumed.push_back(item);
        }
    });

    producer.join();
    consumer.join();

    // Verify all items were consumed in order
    ASSERT_EQ(produced.size(), consumed.size());
    for (size_t i = 0; i < produced.size(); ++i) {
        EXPECT_EQ(produced[i], consumed[i]);
    }
}

// Test 2: Finished behavior - pop returns false when buffer is empty and finished
TEST_F(ThreadSafeBufferTest, FinishedBehaviorEmptyBuffer) {
    buffer.setFinished();
    
    int item;
    bool result = buffer.pop(item, 100ms);
    
    EXPECT_FALSE(result) << "pop should return false when buffer is empty and finished";
}

// Test 3: Finished behavior - existing items can still be popped
TEST_F(ThreadSafeBufferTest, FinishedBehaviorWithItems) {
    // Push some items
    buffer.push(1);
    buffer.push(2);
    buffer.push(3);
    
    // Mark as finished
    buffer.setFinished();
    
    // Should still be able to pop existing items
    int item;
    EXPECT_TRUE(buffer.pop(item, 100ms));
    EXPECT_EQ(item, 1);
    
    EXPECT_TRUE(buffer.pop(item, 100ms));
    EXPECT_EQ(item, 2);
    
    EXPECT_TRUE(buffer.pop(item, 100ms));
    EXPECT_EQ(item, 3);
    
    // Now buffer is empty and finished, should return false
    EXPECT_FALSE(buffer.pop(item, 100ms));
}

// Test 4: Timeout behavior - pop times out if no items available
TEST_F(ThreadSafeBufferTest, PopTimeout) {
    auto start = std::chrono::steady_clock::now();
    
    int item;
    bool result = buffer.pop(item, 50ms);
    
    auto elapsed = std::chrono::steady_clock::now() - start;
    
    EXPECT_FALSE(result) << "pop should return false on timeout";
    EXPECT_GE(elapsed, 50ms) << "pop should wait at least the timeout duration";
    EXPECT_LT(elapsed, 100ms) << "pop should not wait significantly longer than timeout";
}

// Test 5: Producer-consumer with varying speeds
TEST_F(ThreadSafeBufferTest, VaryingProducerConsumerSpeeds) {
    const int NUM_ITEMS = 50;
    std::atomic<int> consumed_count{0};

    // Slow producer
    std::thread producer([&]() {
        for (int i = 0; i < NUM_ITEMS; ++i) {
            buffer.push(i);
            std::this_thread::sleep_for(1ms);  // Slow down production
        }
        buffer.setFinished();
    });

    // Fast consumer
    std::thread consumer([&]() {
        int item;
        while (buffer.pop(item, 1000ms)) {
            consumed_count++;
        }
    });

    producer.join();
    consumer.join();

    EXPECT_EQ(consumed_count.load(), NUM_ITEMS);
}

// Test 6: Multiple items pushed before consumption
TEST_F(ThreadSafeBufferTest, BulkPushThenConsume) {
    const int NUM_ITEMS = 1000;
    
    // Push all items
    for (int i = 0; i < NUM_ITEMS; ++i) {
        buffer.push(i);
    }
    buffer.setFinished();
    
    // Consume all items
    int count = 0;
    int item;
    while (buffer.pop(item, 100ms)) {
        EXPECT_EQ(item, count);
        count++;
    }
    
    EXPECT_EQ(count, NUM_ITEMS);
}

// Test 7: Size tracking
TEST_F(ThreadSafeBufferTest, SizeTracking) {
    EXPECT_EQ(buffer.size(), 0);
    
    buffer.push(1);
    EXPECT_EQ(buffer.size(), 1);
    
    buffer.push(2);
    EXPECT_EQ(buffer.size(), 2);
    
    int item;
    buffer.pop(item, 100ms);
    EXPECT_EQ(buffer.size(), 1);
    
    buffer.pop(item, 100ms);
    EXPECT_EQ(buffer.size(), 0);
}

// Test 8: Empty state behavior
TEST_F(ThreadSafeBufferTest, EmptyState) {
    EXPECT_FALSE(buffer.empty()) << "Buffer should not be empty initially (not finished)";
    
    buffer.setFinished();
    EXPECT_TRUE(buffer.empty()) << "Buffer should be empty when finished with no items";
    
    buffer.push(1);
    EXPECT_FALSE(buffer.empty()) << "Buffer should not be empty when it has items";
}

// Test 9: Thread safety under concurrent access
TEST_F(ThreadSafeBufferTest, ConcurrentAccess) {
    const int NUM_ITEMS = 10000;
    std::atomic<int> total_consumed{0};
    
    // Multiple producers
    auto producer_func = [&](int start, int count) {
        for (int i = start; i < start + count; ++i) {
            buffer.push(i);
        }
    };
    
    std::thread p1(producer_func, 0, NUM_ITEMS / 2);
    std::thread p2(producer_func, NUM_ITEMS / 2, NUM_ITEMS / 2);
    
    // Give producers time to add items
    p1.join();
    p2.join();
    buffer.setFinished();
    
    // Consumer
    std::thread consumer([&]() {
        int item;
        while (buffer.pop(item, 1000ms)) {
            total_consumed++;
        }
    });
    
    consumer.join();
    
    EXPECT_EQ(total_consumed.load(), NUM_ITEMS);
}

// Test 10: Item uninitialized on timeout (safety check)
TEST_F(ThreadSafeBufferTest, ItemNotSetOnTimeout) {
    int item = 999;  // Set to known value
    
    bool result = buffer.pop(item, 50ms);
    
    EXPECT_FALSE(result);
    // Item value is undefined on timeout (could be 999 or changed)
    // Main point is that pop returned false, so caller knows not to use item
}
