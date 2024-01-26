/**
Copyright 2021 JasmineGraph Team
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

#include "AbstractExecutor.h"
#include "../../../performance/metrics/PerformanceUtil.h"

AbstractExecutor::AbstractExecutor(JobRequest jobRequest) { this->request = jobRequest; }

AbstractExecutor::AbstractExecutor() {}

std::vector<std::vector<string>> AbstractExecutor::getCombinations(std::vector<string> inputVector) {
    std::vector<std::vector<string>> combinationsList;
    std::vector<std::vector<int>> combinations;

    // Below algorithm will get all the combinations of 3 workers for given set of workers
    std::string bitmask(3, 1);
    bitmask.resize(inputVector.size(), 0);

    do {
        std::vector<int> combination;
        for (int i = 0; i < inputVector.size(); ++i) {
            if (bitmask[i]) {
                combination.push_back(i);
            }
        }
        combinations.push_back(combination);
    } while (std::prev_permutation(bitmask.begin(), bitmask.end()));

    for (std::vector<std::vector<int>>::iterator combinationsIterator = combinations.begin();
         combinationsIterator != combinations.end(); ++combinationsIterator) {
        std::vector<int> combination = *combinationsIterator;
        std::vector<string> tempWorkerIdCombination;

        for (std::vector<int>::iterator combinationIterator = combination.begin();
             combinationIterator != combination.end(); ++combinationIterator) {
            int index = *combinationIterator;

            tempWorkerIdCombination.push_back(inputVector.at(index));
        }

        combinationsList.push_back(tempWorkerIdCombination);
    }

    return combinationsList;
}

int AbstractExecutor::collectPerformaceData(PerformanceSQLiteDBInterface *perDB, std::string graphId,
                                            std::string command, std::string category, int partitionCount,
                                            std::string masterIP, bool autoCalibrate) {
    int elapsedTime = 0;
    time_t start;
    time_t end;
    PerformanceUtil::init();

    std::vector<Place> placeList = PerformanceUtil::getHostReporterList();
    std::string slaCategoryId = PerformanceUtil::getSLACategoryId(command, category);

    std::vector<Place>::iterator placeListIterator;

    for (placeListIterator = placeList.begin(); placeListIterator != placeList.end(); ++placeListIterator) {
        Place place = *placeListIterator;
        std::string host;

        std::string ip = place.ip;
        std::string user = place.user;
        std::string serverPort = place.serverPort;
        std::string isMaster = place.isMaster;
        std::string isHostReporter = place.isHostReporter;
        std::string hostId = place.hostId;
        std::string placeId = place.placeId;

        if (ip.find("localhost") != std::string::npos || ip.compare(masterIP) == 0) {
            host = ip;
        } else {
            host = user + "@" + ip;
        }

        if (!(isMaster.find("true") != std::string::npos || host == "localhost" || host.compare(masterIP) == 0)) {
            PerformanceUtil::initiateCollectingRemoteSLAResourceUtilization(
                    host, atoi(serverPort.c_str()), isHostReporter, "false",
                    placeId, elapsedTime, masterIP);
        }
    }

    start = time(0);
    PerformanceUtil::collectSLAResourceConsumption(placeList, graphId, command, category, masterIP, elapsedTime,
                                                   autoCalibrate);

    while (!workerResponded) {
        time_t elapsed = time(0) - start;
        if (elapsed >= Conts::LOAD_AVG_COLLECTING_GAP) {
            elapsedTime += Conts::LOAD_AVG_COLLECTING_GAP * 1000;
            PerformanceUtil::collectSLAResourceConsumption(placeList, graphId, command, category, masterIP, elapsedTime,
                                                           autoCalibrate);
            start = start + Conts::LOAD_AVG_COLLECTING_GAP;
        } else {
            sleep(Conts::LOAD_AVG_COLLECTING_GAP - elapsed);
        }
    }

    PerformanceUtil::updateRemoteResourceConsumption(perDB, graphId, partitionCount, placeList,
                                                     slaCategoryId, masterIP);
    PerformanceUtil::updateResourceConsumption(perDB, graphId, partitionCount, placeList, slaCategoryId);

    return 0;
}
