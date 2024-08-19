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

#ifndef HDFSCONNECTOR_H
#define HDFSCONNECTOR_H

#include <string>
#include "../logger/Logger.h"
#include "hdfs.h"

class HDFSConnector {
public:
    HDFSConnector(const std::string &hdfsServerIP, const std::string &hdfsServerPort);
    ~HDFSConnector();
    hdfsFS getFileSystem();

private:
    hdfsFS fileSystem;
    Logger frontend_logger;
};

#endif // HDFSCONNECTOR_H
