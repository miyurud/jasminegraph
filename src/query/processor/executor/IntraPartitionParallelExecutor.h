/**
 * Copyright 2025 JasmineGraph Team
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef INTRA_PARTITION_PARALLEL_EXECUTOR_H
#define INTRA_PARTITION_PARALLEL_EXECUTOR_H

#include <vector>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <queue>
#include <functional>
#include <thread>
#include <future>
#include <algorithm>
#include <unordered_set>
#include "../../../performance/metrics/StatisticsCollector.h"

/**
 * Thread-safe node data structure for parallel processing
 * Holds pre-loaded node data to avoid thread safety issues with NodeManager
 */
struct PreloadedNodeData {
    std::string nodeId;
    std::string partitionId;
    std::map<std::string, std::string, std::less<>> properties;

    PreloadedNodeData(const std::string& id, const std::string& partId,
                      const std::map<std::string, std::string, std::less<>>& props)
        : nodeId(id), partitionId(partId), properties(props) {}
};

/**
 * Thread-safe buffer for managing parallel access to partition data
 */
template<typename T>
class ThreadSafeBuffer {
 private:
    std::queue<T> buffer;
    mutable std::mutex bufferMutex;
    std::condition_variable cv;
    bool finished = false;

 public:
    void push(const T& item) {
        std::scoped_lock lock(bufferMutex);
        buffer.push(item);
        cv.notify_one();
    }

    bool pop(T& item, std::chrono::milliseconds timeout = std::chrono::milliseconds(100)) {
        std::unique_lock lock(bufferMutex);
        cv.wait_for(lock, timeout, [this] {
            return !buffer.empty() || finished;
        });

        if (!buffer.empty()) {
            item = buffer.front();
            buffer.pop();
            return true;
        }
        return false; 
    }

    void setFinished() {
        std::scoped_lock lock(bufferMutex);
        finished = true;
        cv.notify_all();
    }

    size_t size() const {
        std::scoped_lock lock(bufferMutex);
        return buffer.size();
    }

    bool empty() const {
        std::scoped_lock lock(bufferMutex);
        return buffer.empty() && finished;
    }
};

/**
 * Represents a chunk of work for parallel processing
 */
struct WorkChunk {
    long startIndex;
    long endIndex;
    size_t estimatedSize;

    WorkChunk(long start, long end, size_t size)
        : startIndex(start), endIndex(end), estimatedSize(size) {}
};

/**
 * Dynamic thread pool that adapts to system capabilities
 * Automatically detects CPU core count and creates optimal number of workers
 */
class DynamicThreadPool {
 private:
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;
    std::mutex queueMutex;
    std::condition_variable condition;
    std::atomic<bool> stop{false};
    int optimalWorkerCount;

 public:
    DynamicThreadPool();
    ~DynamicThreadPool();

    template<class F, class... Args>
    auto enqueue(F&& f, Args&&... args)
        -> std::future<typename std::invoke_result_t<F, Args...>>;

    int getWorkerCount() const { return optimalWorkerCount; }

 private:
    void workerFunction();
};

/**
 * Intra-Partition Parallel Executor
 * Main class for executing queries in parallel within a single partition
 * Provides automatic hardware detection and adaptive execution
 */
class IntraPartitionParallelExecutor {
 private:
    std::unique_ptr<DynamicThreadPool> threadPool;
    int workerCount;

    // Adaptive chunk size calculation based on data size and worker count
    size_t calculateOptimalChunkSize(size_t totalItems, int workers) const;

 public:
    IntraPartitionParallelExecutor();
    ~IntraPartitionParallelExecutor() = default;

    /**
     * Execute a task across multiple chunks in parallel
     * Template allows for different task types and result types
     */
    template<typename TaskFunc, typename ResultType>
    std::vector<ResultType> executeChunkedTasks(
        const std::vector<WorkChunk>& chunks,
        TaskFunc taskFunction);

    /**
     * Process items in parallel within a partition
     * Automatically divides work into optimal chunks
     */
    template<typename Processor, typename ResultType>
    std::vector<ResultType> processInParallel(
        long totalItemCount,
        Processor processor);

    /**
     * Check if parallel processing is beneficial for given data size
     * Small datasets should use sequential processing to avoid overhead
     */
    bool shouldUseParallelProcessing(size_t dataSize) const;

    /**
     * Get the number of worker threads (equals CPU core count)
     */
    int getWorkerCount() const { return workerCount; }

    /**
     * Performance metrics for monitoring and tuning
     */
    struct PerformanceMetrics {
        int workerCount;
        size_t estimatedSpeedup;
        double cpuUtilization;
        size_t optimalChunkSize;
    };

    PerformanceMetrics getPerformanceMetrics(size_t dataSize) const;

    /**
     * Create work chunks for parallel processing
     */
    std::vector<WorkChunk> createWorkChunks(long totalItems, size_t chunkSize) const;

    /**
     * Merge results from multiple chunks with deduplication
     */
    template<typename T>
    std::vector<T> mergeResults(const std::vector<std::vector<T>>& chunkResults) const;
};

// Template implementations

template<class F, class... Args>
auto DynamicThreadPool::enqueue(F&& f, Args&&... args)
    -> std::future<typename std::invoke_result_t<F, Args...>> {
    using return_type = typename std::invoke_result_t<F, Args...>;

    auto task = std::make_shared<std::packaged_task<return_type()>>(
        std::bind(std::forward<F>(f), std::forward<Args>(args)...));

    std::future<return_type> res = task->get_future();
    {
        std::scoped_lock lock(queueMutex);
        if (stop)
            throw std::runtime_error("enqueue on stopped ThreadPool");
        tasks.emplace([task](){ (*task)(); });
    }
    condition.notify_one();
    return res;
}

template<typename TaskFunc, typename ResultType>
std::vector<ResultType> IntraPartitionParallelExecutor::executeChunkedTasks(
    const std::vector<WorkChunk>& chunks,
    TaskFunc taskFunction) {

    std::vector<std::future<ResultType>> futures;
    futures.reserve(chunks.size());

    // Submit all chunks to thread pool
    for (const auto& chunk : chunks) {
        futures.push_back(
            threadPool->enqueue([chunk, taskFunction]() -> ResultType {
                return taskFunction(chunk);
            }));
    }

    // Collect results
    std::vector<ResultType> results;
    results.reserve(chunks.size());

    for (auto& future : futures) {
        results.push_back(future.get());
    }

    return results;
}

template<typename Processor, typename ResultType>
std::vector<ResultType> IntraPartitionParallelExecutor::processInParallel(
    long totalItemCount,
    Processor processor) {

    // Calculate optimal chunk size
    size_t chunkSize = calculateOptimalChunkSize(totalItemCount, workerCount);

    // Create work chunks
    std::vector<WorkChunk> chunks = createWorkChunks(totalItemCount, chunkSize);

    // Execute in parallel
    return executeChunkedTasks<Processor, ResultType>(chunks, processor);
}

template<typename T>
std::vector<T> IntraPartitionParallelExecutor::mergeResults(
    const std::vector<std::vector<T>>& chunkResults) const {

    // Estimate total size for efficiency
    size_t totalSize = 0;
    for (const auto& chunk : chunkResults) {
        totalSize += chunk.size();
    }

    std::vector<T> finalResult;
    finalResult.reserve(totalSize);

    // Merge all chunk results
    for (const auto& chunk : chunkResults) {
        finalResult.insert(finalResult.end(), chunk.begin(), chunk.end());
    }

    return finalResult;
}

#endif  // INTRA_PARTITION_PARALLEL_EXECUTOR_H
