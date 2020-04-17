/**
Copyright 2018 JasmineGraph Team
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

#include "PlacesToNodeMapper.h"
#include "logger/Logger.h"

using namespace std;
Logger node_logger;

std::string PlacesToNodeMapper::getHost(long placeId) {
    Utils utils;
    std::vector<std::string> hostList = utils.getHostListFromProperties();
    std::string & host = hostList.at(placeId);
    return host;
}

std::vector<int> PlacesToNodeMapper::getInstancePortsList(long placeId) {
    Utils utils;
    int numberOfWorkersPerHost;
    int numberOfWorkers =0;
    int hostListModeNWorkers;
    std::vector<int> portList;
    std::vector<std::string> hostList = utils.getHostListFromProperties();
    std::string nWorkers = utils.getJasmineGraphProperty("org.jasminegraph.server.nworkers");
    int workerPort = Conts::JASMINEGRAPH_INSTANCE_PORT;
    if (utils.is_number(nWorkers)) {
        numberOfWorkers = atoi(nWorkers.c_str());
    } else {
        node_logger.log("Number of Workers is not specified", "error");
        numberOfWorkers = 0;
    }

    if (numberOfWorkers > 0 && hostList.size() > 0) {
        numberOfWorkersPerHost = numberOfWorkers/hostList.size();
        hostListModeNWorkers = numberOfWorkers % hostList.size();
    }

    if (placeId > 0 && (placeId+1)<=hostListModeNWorkers) {
        workerPort = workerPort + (placeId*numberOfWorkersPerHost*2) + placeId*2 + 2;
    } else if (placeId > 0 && (placeId+1)>hostListModeNWorkers) {
        workerPort = workerPort + (placeId*numberOfWorkersPerHost*2) + hostListModeNWorkers*2 + 2;
    }

    for (int i=0; i<numberOfWorkersPerHost;i++) {
        workerPort = Conts::JASMINEGRAPH_INSTANCE_PORT + i*2;
        portList.push_back(workerPort);
    }

    if ((placeId+1) <= hostListModeNWorkers) {
        workerPort = workerPort + 2;
        portList.push_back(workerPort);
    }

    return portList;

}

std::vector<int> PlacesToNodeMapper::getFileTransferServicePort(long placeId) {
    Utils utils;
    int numberOfWorkersPerHost;
    int numberOfWorkers =0;
    int hostListModeNWorkers;
    std::vector<int> portList;
    std::vector<std::string> hostList = utils.getHostListFromProperties();
    std::string nWorkers = utils.getJasmineGraphProperty("org.jasminegraph.server.nworkers");
    int workerPort = Conts::JASMINEGRAPH_INSTANCE_PORT;
    int workerDataPort = Conts::JASMINEGRAPH_INSTANCE_DATA_PORT;
    if (utils.is_number(nWorkers)) {
        numberOfWorkers = atoi(nWorkers.c_str());
    } else {
        node_logger.log("Number of Workers is not specified", "error");
        numberOfWorkers = 0;
    }

    if (numberOfWorkers > 0 && hostList.size() > 0) {
        numberOfWorkersPerHost = numberOfWorkers/hostList.size();
        hostListModeNWorkers = numberOfWorkers % hostList.size();
    }

    if (placeId > 0 && (placeId+1)<=hostListModeNWorkers) {
        workerDataPort = workerDataPort + (placeId*numberOfWorkersPerHost*2) + placeId*2 + 2;
    } else if (placeId > 0 && (placeId+1)>hostListModeNWorkers) {
        workerDataPort = workerDataPort + (placeId*numberOfWorkersPerHost*2) + hostListModeNWorkers*2 + 2;
    }

    for (int i=0; i<numberOfWorkersPerHost;i++) {
        workerDataPort = workerDataPort + i*2;
        portList.push_back(workerDataPort);
    }

    if ((placeId+1) <= hostListModeNWorkers) {
        workerDataPort = workerDataPort + 2;
        portList.push_back(workerDataPort);
    }

    return portList;
}