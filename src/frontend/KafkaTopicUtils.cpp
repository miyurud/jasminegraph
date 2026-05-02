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

#include "KafkaTopicUtils.h"

namespace KafkaTopicUtils {

std::string extractTopicName(const std::string &uploadPath) {
    if (uploadPath.empty()) {
        return "";
    }

    size_t prefix = uploadPath.find("kafka:");
    if (prefix == std::string::npos) {
        return "";
    }

    std::string rest = uploadPath.substr(prefix + 6);
    if (!rest.empty() && (rest[0] == '\\' || rest[0] == '/')) {
        rest = rest.substr(1);
    }

    size_t separator = rest.find(':');
    std::string topic = separator == std::string::npos ? rest : rest.substr(0, separator);
    return topic;
}

std::set<std::string> extractTopicNames(
    const std::vector<std::vector<std::pair<std::string, std::string>>> &rows) {
    std::set<std::string> topicNames;

    for (const auto &row : rows) {
        if (row.empty()) {
            continue;
        }

        std::string topicName = extractTopicName(row[0].second);
        if (!topicName.empty()) {
            topicNames.insert(topicName);
        }
    }

    return topicNames;
}

std::string serializeTopicNames(const std::set<std::string> &topicNames,
                                const std::string &lineEnding) {
    std::string serialized;
    for (const auto &topicName : topicNames) {
        serialized += topicName + lineEnding;
    }
    return serialized;
}

}  // namespace KafkaTopicUtils
