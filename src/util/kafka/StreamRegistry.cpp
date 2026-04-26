/**
Copyright 2019 JasminGraph Team
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

#include "StreamRegistry.h"

#include <iostream>

StreamRegistry &StreamRegistry::getInstance() {
    static StreamRegistry instance;
    return instance;
}

bool StreamRegistry::registerStream(int graphId, const std::string &topicName, int connectionFd,
                                     KafkaConnector *kafkaConnector, const std::string &userId) {
    std::lock_guard<std::mutex> lock(registryMutex);

    // Check if stream with this graphId already exists
    if (activeStreams.find(graphId) != activeStreams.end()) {
        std::cerr << "StreamRegistry: Stream with graphId " << graphId << " already registered" << std::endl;
        return false;
    }

    // Create new stream metadata
    auto metadata = std::make_shared<StreamMetadata>(graphId, topicName, connectionFd,
                                                     std::this_thread::get_id(), kafkaConnector, userId);
    activeStreams[graphId] = metadata;

    std::cout << "StreamRegistry: Registered stream for graphId=" << graphId << ", topic=" << topicName
              << ", connectionFd=" << connectionFd << std::endl;

    return true;
}

void StreamRegistry::updateStreamThreadId(int graphId, std::thread::id threadId) {
    std::lock_guard<std::mutex> lock(registryMutex);

    auto it = activeStreams.find(graphId);
    if (it != activeStreams.end()) {
        it->second->threadId = threadId;
        std::cout << "StreamRegistry: Updated thread ID for graphId=" << graphId << std::endl;
    } else {
        std::cerr << "StreamRegistry: Cannot update thread ID - stream graphId " << graphId
                  << " not found" << std::endl;
    }
}

std::shared_ptr<StreamMetadata> StreamRegistry::getStreamByGraphId(int graphId) {
    std::lock_guard<std::mutex> lock(registryMutex);

    auto it = activeStreams.find(graphId);
    if (it != activeStreams.end()) {
        return it->second;
    }
    return nullptr;
}

std::map<int, std::shared_ptr<StreamMetadata>> StreamRegistry::getAllStreams() {
    std::lock_guard<std::mutex> lock(registryMutex);
    return activeStreams;
}

bool StreamRegistry::unregisterStream(int graphId) {
    std::lock_guard<std::mutex> lock(registryMutex);

    auto it = activeStreams.find(graphId);
    if (it != activeStreams.end()) {
        activeStreams.erase(it);
        std::cout << "StreamRegistry: Unregistered stream for graphId=" << graphId << std::endl;
        return true;
    }

    std::cerr << "StreamRegistry: Cannot unregister - stream graphId " << graphId << " not found" << std::endl;
    return false;
}

bool StreamRegistry::signalStreamStop(int graphId) {
    std::lock_guard<std::mutex> lock(registryMutex);

    auto it = activeStreams.find(graphId);
    if (it != activeStreams.end()) {
        it->second->stopFlag->store(true, std::memory_order_release);
        std::cout << "StreamRegistry: Signaled stop for graphId=" << graphId << std::endl;
        return true;
    }

    std::cerr << "StreamRegistry: Cannot signal stop - stream graphId " << graphId << " not found" << std::endl;
    return false;
}

bool StreamRegistry::isStreamActive(int graphId) {
    std::lock_guard<std::mutex> lock(registryMutex);
    return activeStreams.find(graphId) != activeStreams.end();
}

size_t StreamRegistry::getActiveStreamCount() {
    std::lock_guard<std::mutex> lock(registryMutex);
    return activeStreams.size();
}

void StreamRegistry::stopAllStreams() {
    std::lock_guard<std::mutex> lock(registryMutex);

    std::cout << "StreamRegistry: Stopping all " << activeStreams.size() << " active streams" << std::endl;

    for (auto &entry : activeStreams) {
        entry.second->stopFlag->store(true, std::memory_order_release);
    }
}

void StreamRegistry::clear() {
    std::lock_guard<std::mutex> lock(registryMutex);
    activeStreams.clear();
    std::cout << "StreamRegistry: Cleared all stream entries" << std::endl;
}
