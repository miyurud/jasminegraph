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

#ifndef JASMINEGRAPH_STREAM_REGISTRY_H
#define JASMINEGRAPH_STREAM_REGISTRY_H

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "KafkaCC.h"

/**
 * StreamMetadata: Holds metadata about a single active Kafka stream
 */
struct StreamMetadata {
    int graphId;                              // Graph ID for this stream
    std::string topicName;                    // Kafka topic name
    int connectionFd;                         // Client connection file descriptor
    std::thread::id threadId;                 // ID of the consumption thread
    std::shared_ptr<std::atomic<bool>> stopFlag;  // Atomic flag to signal stream stop
    KafkaConnector *kafkaConnector;           // Pointer to the Kafka consumer
    std::string userId;                       // User ID or session identifier
    std::chrono::system_clock::time_point startTime;  // Time when stream started

    StreamMetadata(int gId, const std::string &topic, int connFd, std::thread::id tid,
                   KafkaConnector *kconn, const std::string &uid)
        : graphId(gId),
          topicName(topic),
          connectionFd(connFd),
          threadId(tid),
          stopFlag(std::make_shared<std::atomic<bool>>(false)),
          kafkaConnector(kconn),
          userId(uid),
          startTime(std::chrono::system_clock::now()) {}
};

/**
 * StreamRegistry: Global registry for managing all active Kafka streams
 * Thread-safe singleton that tracks stream lifecycle
 */
class StreamRegistry {
 public:
    /**
     * Get the singleton instance of StreamRegistry
     */
    static StreamRegistry &getInstance();

    /**
     * Register a new stream in the registry
     * @param graphId Graph ID for the stream
     * @param topicName Kafka topic name
     * @param connectionFd Client connection file descriptor
     * @param kafkaConnector Pointer to KafkaConnector
     * @param userId User identifier for this stream
     * @return true if registration successful, false if graphId already registered
     */
    bool registerStream(int graphId, const std::string &topicName, int connectionFd,
                        KafkaConnector *kafkaConnector, const std::string &userId);

    /**
     * Update the stream thread ID (call after spawning consumption thread)
     * @param graphId Graph ID
     * @param threadId Thread ID of the consumption thread
     */
    void updateStreamThreadId(int graphId, std::thread::id threadId);

    /**
     * Get stream metadata by graph ID
     * @param graphId Graph ID to look up
     * @return Pointer to StreamMetadata if found, nullptr otherwise
     */
    std::shared_ptr<StreamMetadata> getStreamByGraphId(int graphId);

    /**
     * Get all active streams
     * @return Map of graphId -> StreamMetadata
     */
    std::map<int, std::shared_ptr<StreamMetadata>> getAllStreams();

    /**
     * Unregister (remove) a stream from the registry
     * @param graphId Graph ID to remove
     * @return true if stream was found and removed, false otherwise
     */
    bool unregisterStream(int graphId);

    /**
     * Signal a stream to stop gracefully
     * @param graphId Graph ID of stream to stop
     * @return true if signal sent, false if stream not found
     */
    bool signalStreamStop(int graphId);

    /**
     * Check if a stream is currently active
     * @param graphId Graph ID to check
     * @return true if stream is registered, false otherwise
     */
    bool isStreamActive(int graphId);

    /**
     * Get the number of active streams
     * @return Count of active streams
     */
    size_t getActiveStreamCount();

    /**
     * Stop all active streams (for shutdown)
     */
    void stopAllStreams();

    /**
     * Clear the registry completely (careful: assumes no active threads)
     */
    void clear();

 private:
    // Private constructor for singleton
    StreamRegistry() = default;

    // Delete copy constructor and assignment operator
    StreamRegistry(const StreamRegistry &) = delete;
    StreamRegistry &operator=(const StreamRegistry &) = delete;

    std::map<int, std::shared_ptr<StreamMetadata>> activeStreams;
    mutable std::mutex registryMutex;
};

#endif  // JASMINEGRAPH_STREAM_REGISTRY_H
