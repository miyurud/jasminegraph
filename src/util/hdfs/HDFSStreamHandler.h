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
#include "../Utils.h"
#include "../../partitioner/stream/HashPartitioner.h"
#include "../../nativestore/DataPublisher.h"

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

    void startStreamingFromBufferToPartitions();

private:
    void streamFromHDFSIntoBuffer();
    void streamFromBufferToProcessingQueue(HashPartitioner &partitioner);

    hdfsFS fileSystem;

    std::string filePath;
    std::queue<std::string> dataBuffer;

    std::mutex dataBufferMutex;
    std::condition_variable dataBufferCV;

    std::string masterIP;
    SQLiteDBInterface *sqlite;

    bool isReading;
    bool isProcessing;
    int graphId;
    int numberOfPartitions;
    std::mutex dbLock;

};

#endif // HDFSSTREAMHANDLER_H