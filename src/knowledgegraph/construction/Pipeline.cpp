//
// Created by sajeenthiran on 2025-08-11.
//

#include "Pipeline.h"
#include <hdfs.h>

#include <iostream>
#include "../../server/JasmineGraphServer.h"
#include "../../partitioner/stream/HDFSMultiThreadedHashPartitioner.h"
#include <chrono>
#include <nlohmann/json.hpp>
#include <string>
#include <sstream>
#include <stdlib.h>
#include <mutex>
#include <queue>
#include <regex>
#include <thread>
class SQLiteDBInterface;

using json = nlohmann::json;
using namespace std;
using namespace std::chrono;

Logger kg_pipeline_stream_handler_logger;

const size_t MESSAGE_SIZE = 5 * 1024 * 1024;
const size_t MAX_BUFFER_SIZE = MESSAGE_SIZE * 512;
const size_t CHUNCK_BYTE_SIZE = 1024 * 1024; // 1 MB chunks
const std::string END_OF_STREAM_MARKER = "-1";

Pipeline::Pipeline(hdfsFS fileSystem, const std::string &filePath, int numberOfPartitions, int graphId,
                                     std::string masterIP)
        : fileSystem(fileSystem),
          filePath(filePath),
          numberOfPartitions(numberOfPartitions),
          isReading(true),
          isProcessing(true),
          graphId(graphId),
          masterIP(masterIP) {}


void Pipeline:: init()
{
    startStreamingFromBufferToWorkers();
}
// ---------- document_reader ----------
// Reads chunks of fixed byte size from an AHDFS file and pushes them as chunks with generated doc_id
void Pipeline::streamFromHDFSIntoBuffer() {
    auto startTime = chrono::high_resolution_clock::now();
    kg_pipeline_stream_handler_logger.info("Started streaming data from HDFS into data buffer...");

    hdfsFile file = hdfsOpenFile(fileSystem, filePath.c_str(), O_RDONLY, 0, 0, 0);
    if (!file) {
        kg_pipeline_stream_handler_logger.error("Failed to open HDFS file.");
        isReading = false;
        dataBufferCV.notify_all();
        return;
    }



    std::vector<char> buffer(CHUNCK_BYTE_SIZE);
    int64_t read_bytes = 0;
    int chunk_idx = 0;
    std::string leftover;

    while ((read_bytes = hdfsRead(fileSystem, file, buffer.data(), CHUNCK_BYTE_SIZE)) > 0) {
        std::string chunk_text = leftover + std::string(buffer.data(), read_bytes);

        // Find last newline to keep only complete lines in chunk pushed to dataBuffer
        size_t last_newline = chunk_text.find_last_of('\n');
        if (last_newline == std::string::npos) {
            // No newline found: entire chunk is partial line, keep as leftover and continue
            leftover = chunk_text;
            continue;
        }

        // Split into complete lines and leftover partial line
        std::string full_lines_chunk = chunk_text.substr(0, last_newline + 1);
        leftover = chunk_text.substr(last_newline + 1);

        // Wait and push to dataBuffer safely
        std::unique_lock<std::mutex> lock(dataBufferMutex);
        dataBufferCV.wait(lock, [this] { return dataBuffer.size() < MAX_BUFFER_SIZE || !isReading; });

        dataBuffer.push(std::move(full_lines_chunk));
        lock.unlock();
        dataBufferCV.notify_one();

        chunk_idx++;
    }

    // Push leftover partial line if any (last chunk)
    if (!leftover.empty()) {
        std::unique_lock<std::mutex> lock(dataBufferMutex);
        dataBufferCV.wait(lock, [this] { return dataBuffer.size() < MAX_BUFFER_SIZE || !isReading; });

        dataBuffer.push(std::move(leftover));
        lock.unlock();
        dataBufferCV.notify_one();
    }

    if (read_bytes < 0) {
        std::cerr << "Error reading from AHDFS file\n";
    }

    hdfsCloseFile(fileSystem, file);
    isReading = false;

    if (!leftover.empty()) {
        std::unique_lock<std::mutex> lock(dataBufferMutex);
        dataBuffer.push(leftover);
        lock.unlock();
        dataBufferCV.notify_all();
    }

    {
        std::unique_lock<std::mutex> lock(dataBufferMutex);
        dataBuffer.push(END_OF_STREAM_MARKER);
        lock.unlock();
    }

    dataBufferCV.notify_all();
    auto endTime = high_resolution_clock::now();
    std::chrono::duration<double> duration = endTime - startTime;
    kg_pipeline_stream_handler_logger.debug("Successfully streamed data from HDFS into data buffer.");
    kg_pipeline_stream_handler_logger.info("Time taken to read from HDFS: " + to_string(duration.count()) + " seconds");
}



// void Pipeline:: streamChunckToWorker(const std::string &chunk, int partitionId) {
//     // Get the worker client for the partition
//     JasmineGraphServer *server = JasmineGraphServer::getInstance();
//     DataPublisher *workerClient = server->getWorkerClientForPartition(graphId, partitionId);
//
//     if (workerClient) {
//         workerClient->publish(chunk);
//     } else {
//         hdfs_stream_handler_logger.error("No worker client found for partition " + std::to_string(partitionId));
//     }
// }

// read from buffer and send to worker in roundRobin
void Pipeline::startStreamingFromBufferToWorkers()
{
    auto startTime = high_resolution_clock::now();
    HDFSMultiThreadedHashPartitioner partitioner(numberOfPartitions, graphId, masterIP, isDirected);

    std::thread readerThread(&Pipeline::streamFromHDFSIntoBuffer, this);
    std::vector<std::thread> bufferProcessorThreads;





    auto endTime = high_resolution_clock::now();
    std::chrono::duration<double> duration = endTime - startTime;
    kg_pipeline_stream_handler_logger.info(
            "Total time taken for streaming from HDFS into partitions: " + to_string(duration.count()) + " seconds");
}


// ---------- split_document ----------
std::vector<Chunk> split_document(const std::string &doc_id, const std::string &text, size_t chunk_words) {
    std::istringstream iss(text);
    std::vector<std::string> words;
    std::string w;
    while (iss >> w) words.push_back(w);
    std::vector<Chunk> out;
    size_t i = 0;
    while (i < words.size()) {
        std::ostringstream oss;
        for (size_t j = 0; j < chunk_words && i < words.size(); ++j, ++i) {
            if (j) oss << ' ';
            oss << words[i];
        }
        out.push_back({doc_id, oss.str()});
    }
    if (out.empty()) out.push_back({doc_id, ""});
    return out;
}
