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

#ifndef JASMINEGRAPH_SRC_FRONTEND_KAFKATOPICUTILS_H_
#define JASMINEGRAPH_SRC_FRONTEND_KAFKATOPICUTILS_H_

#include <set>
#include <string>
#include <utility>
#include <vector>

namespace KafkaTopicUtils {

std::string extractTopicName(const std::string &uploadPath);

std::set<std::string> extractTopicNames(
    const std::vector<std::vector<std::pair<std::string, std::string>>> &rows);

std::string serializeTopicNames(const std::set<std::string> &topicNames,
                                const std::string &lineEnding);

}  // namespace KafkaTopicUtils

#endif  // JASMINEGRAPH_SRC_FRONTEND_KAFKATOPICUTILS_H_
