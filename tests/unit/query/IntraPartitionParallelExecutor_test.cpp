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
#include <vector>
#include <algorithm>

// Test fixture for IntraPartitionParallelExecutor
class IntraPartitionParallelExecutorTest : public ::testing::Test {
protected:
    std::unique_ptr<IntraPartitionParallelExecutor> executor;

    void SetUp() override {
        executor = std::make_unique<IntraPartitionParallelExecutor>();
    }
};

// Test 1: Small dataset threshold check - should use sequential processing
TEST_F(IntraPartitionParallelExecutorTest, SmallDatasetThreshold) {
    size_t smallDataSize = 500;  // Below threshold for most systems
    
    bool useParallel = executor->shouldUseParallelProcessing(smallDataSize);
    
    EXPECT_FALSE(useParallel) << "Small datasets should not use parallel processing";
}

// Test 2: Large dataset should use parallel processing
TEST_F(IntraPartitionParallelExecutorTest, LargeDatasetThreshold) {
    size_t largeDataSize = 10000;
    
    bool useParallel = executor->shouldUseParallelProcessing(largeDataSize);
    
    EXPECT_TRUE(useParallel) << "Large datasets should use parallel processing";
}

// Test 3: Worker count should be positive
TEST_F(IntraPartitionParallelExecutorTest, WorkerCountPositive) {
    int workerCount = executor->getWorkerCount();
    
    EXPECT_GT(workerCount, 0) << "Worker count should be at least 1";
    EXPECT_LE(workerCount, 64) << "Worker count should be reasonable (not exceed typical core counts)";
}

// Test 4: Chunk creation with valid parameters
TEST_F(IntraPartitionParallelExecutorTest, CreateWorkChunksValid) {
    long totalItems = 1000;
    size_t chunkSize = 250;
    
    std::vector<WorkChunk> chunks = executor->createWorkChunks(totalItems, chunkSize);
    
    // Should create 4 chunks: [0-249], [250-499], [500-749], [750-999]
    EXPECT_EQ(chunks.size(), 4);
    
    // Verify first chunk
    EXPECT_EQ(chunks[0].startIndex, 0);
    EXPECT_EQ(chunks[0].endIndex, 249);
    
    // Verify last chunk
    EXPECT_EQ(chunks[3].startIndex, 750);
    EXPECT_EQ(chunks[3].endIndex, 999);
}

// Test 5: Chunk creation with non-divisible size
TEST_F(IntraPartitionParallelExecutorTest, CreateWorkChunksNonDivisible) {
    long totalItems = 1000;
    size_t chunkSize = 300;
    
    std::vector<WorkChunk> chunks = executor->createWorkChunks(totalItems, chunkSize);
    
    // Should create 4 chunks: [0-299], [300-599], [600-899], [900-999]
    EXPECT_EQ(chunks.size(), 4);
    
    // Last chunk should be smaller
    EXPECT_EQ(chunks[3].startIndex, 900);
    EXPECT_EQ(chunks[3].endIndex, 999);
    EXPECT_LT(chunks[3].estimatedSize, chunkSize);
}

// Test 6: Chunk coverage - all items covered exactly once
TEST_F(IntraPartitionParallelExecutorTest, ChunkCoverageComplete) {
    long totalItems = 5000;
    size_t chunkSize = 500;
    
    std::vector<WorkChunk> chunks = executor->createWorkChunks(totalItems, chunkSize);
    
    // Track which items are covered
    std::vector<bool> covered(totalItems, false);
    
    for (const auto& chunk : chunks) {
        for (long i = chunk.startIndex; i <= chunk.endIndex; ++i) {
            ASSERT_FALSE(covered[i]) << "Item " << i << " covered multiple times";
            covered[i] = true;
        }
    }
    
    // Verify all items covered
    for (long i = 0; i < totalItems; ++i) {
        EXPECT_TRUE(covered[i]) << "Item " << i << " not covered by any chunk";
    }
}

// Test 7: Execute chunked tasks - simple sum operation
TEST_F(IntraPartitionParallelExecutorTest, ExecuteChunkedTasksSum) {
    long totalItems = 1000;
    size_t chunkSize = 250;
    
    std::vector<WorkChunk> chunks = executor->createWorkChunks(totalItems, chunkSize);
    
    // Task: sum numbers in each chunk
    auto sumTask = [](const WorkChunk& chunk) -> long {
        long sum = 0;
        for (long i = chunk.startIndex; i <= chunk.endIndex; ++i) {
            sum += i;
        }
        return sum;
    };
    
    std::vector<long> results = executor->executeChunkedTasks<decltype(sumTask), long>(chunks, sumTask);
    
    // Verify we got results for all chunks
    EXPECT_EQ(results.size(), chunks.size());
    
    // Calculate total sum
    long totalSum = 0;
    for (long result : results) {
        totalSum += result;
    }
    
    // Expected sum: 0 + 1 + 2 + ... + 999 = 999 * 1000 / 2 = 499500
    long expectedSum = (totalItems - 1) * totalItems / 2;
    EXPECT_EQ(totalSum, expectedSum);
}

// Test 8: Merge results without duplicates
TEST_F(IntraPartitionParallelExecutorTest, MergeResultsNoDuplicates) {
    std::vector<std::vector<int>> chunkResults = {
        {1, 2, 3},
        {4, 5, 6},
        {7, 8, 9}
    };
    
    std::vector<int> merged = executor->mergeResults(chunkResults);
    
    // Should have all 9 elements
    EXPECT_EQ(merged.size(), 9);
    
    // Verify elements are in order
    for (size_t i = 0; i < merged.size(); ++i) {
        EXPECT_EQ(merged[i], static_cast<int>(i + 1));
    }
}

// Test 9: Performance metrics accuracy
TEST_F(IntraPartitionParallelExecutorTest, PerformanceMetrics) {
    size_t dataSize = 10000;
    
    auto metrics = executor->getPerformanceMetrics(dataSize);
    
    EXPECT_EQ(metrics.workerCount, executor->getWorkerCount());
    EXPECT_GT(metrics.optimalChunkSize, 0);
    EXPECT_GT(metrics.estimatedSpeedup, 0);
    EXPECT_GT(metrics.cpuUtilization, 0.0);
    EXPECT_LE(metrics.cpuUtilization, 100.0);
}

// Test 10: Optimal chunk size calculation
TEST_F(IntraPartitionParallelExecutorTest, OptimalChunkSizeReasonable) {
    // Test with different data sizes
    std::vector<size_t> dataSizes = {1000, 10000, 100000, 1000000};
    
    for (size_t dataSize : dataSizes) {
        auto metrics = executor->getPerformanceMetrics(dataSize);
        size_t chunkSize = metrics.optimalChunkSize;
        
        // Chunk size should be reasonable
        EXPECT_GE(chunkSize, 1000) << "Chunk size too small for data size " << dataSize;
        EXPECT_LE(chunkSize, 100000) << "Chunk size too large for data size " << dataSize;
        
        // Chunks should enable parallel execution
        size_t numChunks = (dataSize + chunkSize - 1) / chunkSize;
        EXPECT_GE(numChunks, static_cast<size_t>(executor->getWorkerCount())) 
            << "Not enough chunks for workers";
    }
}

// Test 11: Process in parallel - correctness test
TEST_F(IntraPartitionParallelExecutorTest, ProcessInParallelCorrectness) {
    long totalItems = 5000;
    
    // Processor: return the square of the index
    auto processor = [](const WorkChunk& chunk) -> std::vector<long> {
        std::vector<long> localResults;
        for (long i = chunk.startIndex; i <= chunk.endIndex; ++i) {
            localResults.push_back(i * i);
        }
        return localResults;
    };
    
    std::vector<std::vector<long>> results = 
        executor->processInParallel<decltype(processor), std::vector<long>>(totalItems, processor);
    
    // Merge results
    std::vector<long> merged = executor->mergeResults(results);
    
    // Should have all items
    EXPECT_EQ(merged.size(), static_cast<size_t>(totalItems));
    
    // Verify each result is correct square
    for (size_t i = 0; i < merged.size(); ++i) {
        long expected = static_cast<long>(i) * static_cast<long>(i);
        EXPECT_EQ(merged[i], expected) << "Mismatch at index " << i;
    }
}

// Test 12: Edge case - single item
TEST_F(IntraPartitionParallelExecutorTest, SingleItemProcessing) {
    long totalItems = 1;
    size_t chunkSize = 100;
    
    std::vector<WorkChunk> chunks = executor->createWorkChunks(totalItems, chunkSize);
    
    EXPECT_EQ(chunks.size(), 1);
    EXPECT_EQ(chunks[0].startIndex, 0);
    EXPECT_EQ(chunks[0].endIndex, 0);
}

// Test 13: Edge case - empty dataset
TEST_F(IntraPartitionParallelExecutorTest, EmptyDataset) {
    long totalItems = 0;
    size_t chunkSize = 100;
    
    std::vector<WorkChunk> chunks = executor->createWorkChunks(totalItems, chunkSize);
    
    EXPECT_EQ(chunks.size(), 0) << "No chunks should be created for empty dataset";
}

// Test 14: Stress test - large dataset processing
TEST_F(IntraPartitionParallelExecutorTest, StressTestLargeDataset) {
    long totalItems = 100000;
    
    auto processor = [](const WorkChunk& chunk) -> long {
        long count = 0;
        for (long i = chunk.startIndex; i <= chunk.endIndex; ++i) {
            count++;
        }
        return count;
    };
    
    std::vector<long> results = 
        executor->processInParallel<decltype(processor), long>(totalItems, processor);
    
    // Sum all counts
    long totalProcessed = 0;
    for (long count : results) {
        totalProcessed += count;
    }
    
    EXPECT_EQ(totalProcessed, totalItems) << "All items should be processed exactly once";
}
