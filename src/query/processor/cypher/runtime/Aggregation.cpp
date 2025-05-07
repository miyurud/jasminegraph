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
#include "Aggregation.h"
#include "../../../../util/logger/Logger.h"
#include <nlohmann/json.hpp>
#include <string>
using json = nlohmann::json;
Logger aggregateLogger;

void AverageAggregation::getResult(int connFd) {
    int result_wr = write(connFd, data.c_str(), data.length());
    if (result_wr < 0) {
        aggregateLogger.error("Error writing to socket");
        return;
    }
    result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (result_wr < 0) {
        aggregateLogger.error("Error writing to socket");
        return;
    }
}

void AverageAggregation::insert(std::string data) {
    json workerData = json::parse(data);
    string variable = workerData["variable"];
    int size = workerData["numberOfData"].get<int>();
    float localAverage = workerData[variable].get<float>();
    if (size > 0) {
        average = (localAverage * size + average * numberOfData) / (numberOfData + size);
    }
    numberOfData += size;
    workerData.erase("variable");
    workerData.erase("numberOfData");
    workerData.erase(variable);
    workerData[variable] = average;
    this->data = workerData.dump();
}

void AscAggregation::insert(string data) {
    this->resultBuffer.add(data);
}


void AscAggregation::getResult(int connFd) {
    while (true) {
        std::string data;
        if (this->resultBuffer.tryGet(data)) {
            if (data == "-1") {
                break;
            }
            int result_wr = write(connFd, data.c_str(), data.length());
            result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(), Conts::CARRIAGE_RETURN_NEW_LINE.size());
            if (result_wr < 0) {
                aggregateLogger.error("Error writing to socket");
                return;
            }
        }
    }
}

