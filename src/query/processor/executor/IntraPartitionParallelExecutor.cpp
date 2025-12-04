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

#include "IntraPartitionParallelExecutor.h"

// DynamicThreadPool Implementation
DynamicThreadPool::DynamicThreadPool() : stop(false) {
    // Auto-detect optimal worker count based on CPU cores
    optimalWorkerCount = StatisticsCollector::getTotalNumberofCores();

    // Safety limits: minimum 1, maximum 32
    optimalWorkerCount = std::max(1, std::min(32, optimalWorkerCount));

    // Create worker threads
    for (int i = 0; i < optimalWorkerCount; ++i) {
        workers.emplace_back([this] { workerFunction(); });
    }
}

DynamicThreadPool::~DynamicThreadPool() {
    {
        std::scoped_lock lock(queueMutex);
        stop = true;
    }
    condition.notify_all();
    for (std::thread &worker : workers) {
        worker.join();
    }
}

void DynamicThreadPool::workerFunction() {
    for (;;) {
        std::function<void()> task;
        {
            std::unique_lock lock(queueMutex);
            condition.wait(lock, [this] { return stop || !tasks.empty(); });
            if (stop && tasks.empty()) {
                return;
            }
            task = std::move(tasks.front());
            tasks.pop();
        }
        task();
    }
}

// IntraPartitionParallelExecutor Implementation
IntraPartitionParallelExecutor::IntraPartitionParallelExecutor() {
    threadPool = std::make_unique<DynamicThreadPool>();
    workerCount = threadPool->getWorkerCount();
}

size_t IntraPartitionParallelExecutor::calculateOptimalChunkSize(size_t totalItems, int workers) const {
    // Target: 2-4 chunks per worker for load balancing
    size_t chunksPerWorker = 3;
    size_t minChunkSize = 1000;    // Minimum to avoid overhead
    size_t maxChunkSize = 100000;  // Maximum to ensure parallelism

    size_t targetChunks = workers * chunksPerWorker;
    size_t chunkSize = totalItems / targetChunks;

    return std::max(minChunkSize, std::min(maxChunkSize, chunkSize));
}

bool IntraPartitionParallelExecutor::shouldUseParallelProcessing(size_t dataSize) const {
    // Use parallel processing for datasets > 1000 items with multiple workers
    return dataSize > 1000 && workerCount > 1;
}

std::vector<WorkChunk> IntraPartitionParallelExecutor::createWorkChunks(long totalItems, size_t chunkSize) const {
    std::vector<WorkChunk> chunks;

    for (long start = 1; start <= totalItems; start += static_cast<long>(chunkSize)) {
        long end = std::min(start + static_cast<long>(chunkSize) - 1, totalItems);
        chunks.emplace_back(start, end, end - start + 1);
    }

    return chunks;
}

IntraPartitionParallelExecutor::PerformanceMetrics
IntraPartitionParallelExecutor::getPerformanceMetrics(size_t dataSize) const {
    PerformanceMetrics metrics;
    metrics.workerCount = workerCount;
    metrics.estimatedSpeedup = shouldUseParallelProcessing(dataSize) ? workerCount : 1;
    metrics.cpuUtilization = shouldUseParallelProcessing(dataSize) ? 100.0 : (100.0 / workerCount);
    metrics.optimalChunkSize = calculateOptimalChunkSize(dataSize, workerCount);
    return metrics;
}
