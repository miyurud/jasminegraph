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

#include "../../../src/frontend/KafkaTopicUtils.h"

#include "gtest/gtest.h"

TEST(KafkaTopicUtilsTest, ExtractTopicNameSupportsKafkaPathFormats) {
    EXPECT_EQ(KafkaTopicUtils::extractTopicName("kafka:\\orders:groupA"), "orders");
    EXPECT_EQ(KafkaTopicUtils::extractTopicName("kafka:/payments:groupB"), "payments");
    EXPECT_EQ(KafkaTopicUtils::extractTopicName("kafka:inventory:groupC"), "inventory");
    EXPECT_EQ(KafkaTopicUtils::extractTopicName("kafka:alerts"), "alerts");
}

TEST(KafkaTopicUtilsTest, ExtractTopicNameHandlesInvalidPaths) {
    EXPECT_EQ(KafkaTopicUtils::extractTopicName(""), "");
    EXPECT_EQ(KafkaTopicUtils::extractTopicName("hdfs:/tmp/graph.dl"), "");
    EXPECT_EQ(KafkaTopicUtils::extractTopicName("kafka:"), "");
}

TEST(KafkaTopicUtilsTest, ExtractTopicNamesDeduplicatesAndIgnoresInvalidRows) {
    std::vector<std::vector<std::pair<std::string, std::string>>> rows = {
        {{"upload_path", "kafka:\\orders:groupA"}},
        {{"upload_path", "kafka:/payments:groupB"}},
        {{"upload_path", "kafka:orders:groupC"}},
        {{"upload_path", "hdfs:/tmp/graph.dl"}},
        {}};

    std::set<std::string> topics = KafkaTopicUtils::extractTopicNames(rows);

    ASSERT_EQ(topics.size(), 2);
    EXPECT_TRUE(topics.find("orders") != topics.end());
    EXPECT_TRUE(topics.find("payments") != topics.end());
}

TEST(KafkaTopicUtilsTest, SerializeTopicNamesFormatsLineSeparatedOutput) {
    std::set<std::string> topics = {"orders", "payments"};

    std::string serialized = KafkaTopicUtils::serializeTopicNames(topics, "\r\n");

    EXPECT_EQ(serialized, "orders\r\npayments\r\n");
}