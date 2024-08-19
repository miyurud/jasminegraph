/*
 * Copyright 2024 JasminGraph Team
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef HDFSSTREAMHANDLER_H
#define HDFSSTREAMHANDLER_H

#include "../logger/Logger.h"
#include "../../partitioner/stream/Partitioner.h"
#include "../../partitioner/local/MetisPartitioner.h"
#include "../Utils.h"

#include <hdfs.h>
#include <vector>
#include <string>
#include <mutex>
#include <queue>
#include <condition_variable>
#include <thread>
#include <fstream>

class HDFSStreamHandler {
public:
    HDFSStreamHandler(hdfsFS fileSystem, const std::string &filePath, int numberOfPartitions, int graphId,
                      SQLiteDBInterface *sqlite,
                      std::string masterIP);

    void start_streaming_data_from_hdfs_into_partitions();

private:
    void stream_from_hdfs_into_buffer();

    void stream_from_buffer_to_processing_queue();

    void process_lines();

    void open_new_file_chunk();

    void load_data_and_close_file_chunk();

    hdfsFS fileSystem;
    MetisPartitioner partitioner;
    std::string filePath;

    std::queue<std::string> dataBuffer;
    std::queue<std::string> lineBuffer;

    std::mutex dataBufferMutex;
    std::mutex lineBufferMutex;

    std::condition_variable dataBufferCV;
    std::condition_variable lineBufferCV;

    std::string masterIP;
    SQLiteDBInterface *sqlite;

    bool isReading;
    bool isProcessing;
    int graphId;
    int numberOfPartitions;

    std::ofstream currentFile;  // Current file being written to
    std::string currentFilePath;  // Path of the current file
    size_t currentFileSize;  // Current size of the file in bytes
    size_t fileIndex;  // Index to keep track of file numbers
};

#endif // HDFSSTREAMHANDLER_H
