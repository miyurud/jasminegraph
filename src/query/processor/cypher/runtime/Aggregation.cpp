//
// Created by kumarawansha on 3/31/25.
//

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
    result_wr = write(connFd, "\r\n", 2);
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
