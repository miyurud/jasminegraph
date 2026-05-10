/**
Copyright 2026 JasmineGraph Team
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

#include "../../../src/util/kafka/StreamRegistry.h"

#include <thread>

#include "gtest/gtest.h"

class StreamRegistryTest : public ::testing::Test {
 protected:
    void SetUp() override { StreamRegistry::getInstance().clear(); }

    void TearDown() override { StreamRegistry::getInstance().clear(); }
};

TEST_F(StreamRegistryTest, RegistersAndReturnsStreamMetadata) {
    StreamRegistry &registry = StreamRegistry::getInstance();

    ASSERT_TRUE(registry.registerStream(10, "topic-a", 77, nullptr, "user-a"));

    EXPECT_TRUE(registry.isStreamActive(10));
    EXPECT_EQ(registry.getActiveStreamCount(), 1);

    auto metadata = registry.getStreamByTopic("topic-a");
    ASSERT_NE(metadata, nullptr);
    EXPECT_EQ(metadata->graphId, 10);
    EXPECT_EQ(metadata->topicName, "topic-a");
    EXPECT_EQ(metadata->connectionFd, 77);
    EXPECT_EQ(metadata->kafkaConnector, nullptr);
    EXPECT_EQ(metadata->userId, "user-a");
    EXPECT_FALSE(metadata->stopFlag->load(std::memory_order_acquire));
}

TEST_F(StreamRegistryTest, RejectsDuplicateGraphIds) {
    StreamRegistry &registry = StreamRegistry::getInstance();

    ASSERT_TRUE(registry.registerStream(10, "topic-a", 77, nullptr, "user-a"));
    EXPECT_FALSE(registry.registerStream(10, "topic-b", 88, nullptr, "user-b"));

    auto allStreams = registry.getAllStreams();
    ASSERT_EQ(allStreams.size(), 1);
    EXPECT_EQ(allStreams.at(10)->topicName, "topic-a");
}

TEST_F(StreamRegistryTest, ReturnsAllStreamsForSharedTopic) {
    StreamRegistry &registry = StreamRegistry::getInstance();

    ASSERT_TRUE(registry.registerStream(10, "shared-topic", 77, nullptr, "user-a"));
    ASSERT_TRUE(registry.registerStream(20, "shared-topic", 88, nullptr, "user-b"));
    ASSERT_TRUE(registry.registerStream(30, "other-topic", 99, nullptr, "user-c"));

    auto sharedStreams = registry.getStreamsByTopic("shared-topic");
    ASSERT_EQ(sharedStreams.size(), 2);
    EXPECT_EQ(sharedStreams[0]->topicName, "shared-topic");
    EXPECT_EQ(sharedStreams[1]->topicName, "shared-topic");
    EXPECT_EQ(registry.getStreamsByTopic("missing-topic").size(), 0);
    EXPECT_EQ(registry.getStreamByTopic("missing-topic"), nullptr);
}

TEST_F(StreamRegistryTest, UpdatesThreadIdForExistingStream) {
    StreamRegistry &registry = StreamRegistry::getInstance();

    ASSERT_TRUE(registry.registerStream(10, "topic-a", 77, nullptr, "user-a"));
    std::thread::id threadId = std::this_thread::get_id();

    registry.updateStreamThreadId(10, threadId);
    registry.updateStreamThreadId(99, threadId);

    auto metadata = registry.getStreamByTopic("topic-a");
    ASSERT_NE(metadata, nullptr);
    EXPECT_EQ(metadata->threadId, threadId);
}

TEST_F(StreamRegistryTest, SignalsAndStopsStreams) {
    StreamRegistry &registry = StreamRegistry::getInstance();

    ASSERT_TRUE(registry.registerStream(10, "topic-a", 77, nullptr, "user-a"));
    ASSERT_TRUE(registry.registerStream(20, "topic-b", 88, nullptr, "user-b"));

    auto first = registry.getStreamByTopic("topic-a");
    auto second = registry.getStreamByTopic("topic-b");
    ASSERT_NE(first, nullptr);
    ASSERT_NE(second, nullptr);

    EXPECT_TRUE(registry.signalStreamStop(10));
    EXPECT_TRUE(first->stopFlag->load(std::memory_order_acquire));
    EXPECT_FALSE(second->stopFlag->load(std::memory_order_acquire));

    registry.stopAllStreams();
    EXPECT_TRUE(second->stopFlag->load(std::memory_order_acquire));
    EXPECT_FALSE(registry.signalStreamStop(99));
}

TEST_F(StreamRegistryTest, UnregistersAndClearsStreams) {
    StreamRegistry &registry = StreamRegistry::getInstance();

    ASSERT_TRUE(registry.registerStream(10, "topic-a", 77, nullptr, "user-a"));
    ASSERT_TRUE(registry.registerStream(20, "topic-b", 88, nullptr, "user-b"));

    EXPECT_TRUE(registry.unregisterStream(10));
    EXPECT_FALSE(registry.isStreamActive(10));
    EXPECT_FALSE(registry.unregisterStream(10));
    EXPECT_EQ(registry.getActiveStreamCount(), 1);

    registry.clear();
    EXPECT_EQ(registry.getActiveStreamCount(), 0);
    EXPECT_TRUE(registry.getAllStreams().empty());
}
