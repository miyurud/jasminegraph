/**
Copyright 2019 JasmineGraph Team
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

#include <sstream>
#include <iostream>
#include "JasmineGraphTrainingSchedular.h"
#include "../metadb/SQLiteDBInterface.h"
#include "../util/Conts.h"
#include "../frontend/JasmineGraphFrontEnd.h"
#include "../performancedb/PerformanceSQLiteDBInterface.h"
#include "../util/logger/Logger.h"

using namespace std;
Logger trainScheduler_logger;

map<string, std::map<int, int>> JasmineGraphTrainingSchedular::schedulePartitionTraining(std::string graphID) {

    map<string, std::map<int, int>> scheduleForEachHost;
    vector<pair<string, string>> hostData;
    SQLiteDBInterface refToSqlite = *new SQLiteDBInterface();
    refToSqlite.init();

    string sqlStatement =
            "SELECT idhost, name FROM host_has_partition INNER JOIN host ON host_idhost = idhost WHERE partition_graph_idgraph = '" +
            graphID + "' group by idhost";
    std::vector<vector<pair<string, string>>> result = refToSqlite.runSelect(sqlStatement);
    for (vector<vector<pair<string, string>>>::iterator i = result.begin(); i != result.end(); ++i) {
        int count = 0;
        string hostID;
        string hostname;
        for (std::vector<pair<string, string>>::iterator j = (i->begin()); j != i->end(); ++j) {
            if (count == 0) {
                hostID = j->second;
            } else {
                hostname = j->second;
                hostData.push_back(pair<string, string>(hostID, hostname));
            }
            count++;
        }
    }
    string id_partition = "";
    string vertexCount = "";
    string centralvertexCount = "";
    int partition_id;
    int vertexcount;
    int centralVertexCount;
    trainScheduler_logger.log("Scheduling training order for each host", "info");
    for (std::vector<pair<string, string>>::iterator j = (hostData.begin()); j != hostData.end(); ++j) {
        sqlStatement =
                "SELECT idpartition,vertexcount,central_vertexcount,graph_idgraph FROM partition INNER JOIN host_has_partition "
                "ON partition.idpartition = host_has_partition.partition_idpartition WHERE graph_idgraph = " + graphID +
                " AND host_idhost = "
                + j->first;
        std::vector<vector<pair<string, string>>> results = refToSqlite.runSelect(sqlStatement);
        std::map<int, int> vertexCountToPartitionId;
        for (std::vector<vector<pair<string, string>>>::iterator i = results.begin(); i != results.end(); ++i) {
            std::vector<pair<string, string>> rowData = *i;
            id_partition = rowData.at(0).second;
            vertexCount = rowData.at(1).second;
            centralvertexCount = rowData.at(2).second;

            partition_id = stoi(id_partition);
            vertexcount = stoi(vertexCount);
            centralVertexCount = stoi(centralvertexCount);

            vertexCountToPartitionId.insert({vertexcount + centralVertexCount, partition_id});


        }
        long availableMemory = getAvailableMemory(j->second);
        vector<pair<int, int>> memoryEstimationForEachPartition;
        for (auto i = vertexCountToPartitionId.begin(); i != vertexCountToPartitionId.end(); ++i) {
            long memoryForPartition = estimateMemory(i->first, graphID);
            trainScheduler_logger.log(
                    "Estimated memory for partition :" + to_string(i->second) + " is " + to_string(memoryForPartition),
                    "info");
            if (memoryForPartition > availableMemory) {
                memoryForPartition = availableMemory;
            }
            memoryEstimationForEachPartition.push_back(make_pair(i->second, (int) memoryForPartition));
        }
        std::map<int, int> scheduledPartitionSets = packPartitionsToMemory(memoryEstimationForEachPartition,
                                                                           availableMemory);
        scheduleForEachHost.insert(make_pair(j->second, scheduledPartitionSets));
    }
    return scheduleForEachHost;
}

long JasmineGraphTrainingSchedular::estimateMemory(int vertexCount, string graph_id) {
    SQLiteDBInterface refToSqlite = *new SQLiteDBInterface();
    refToSqlite.init();

    string sqlStatement =
            "SELECT feature_count FROM graph WHERE idgraph = " + graph_id;
    std::vector<vector<pair<string, string>>> result = refToSqlite.runSelect(sqlStatement);

    cout << sqlStatement << endl;
    int feature_Count;

    for (std::vector<vector<pair<string, string>>>::iterator i = result.begin(); i != result.end(); ++i) {
        std::vector<pair<string, string>> rowData = *i;

        string featureCount = rowData.at(0).second;

        feature_Count = stoi(featureCount);
    }


    long featureMatrixSize;
    long adjacencyMatrixSize;
    long degreeMatrixSize;
    long embeddingMatrixSize;
    long networkXgraphsize;

    featureMatrixSize = 4 * 16 * feature_Count * (vertexCount + 1);
    adjacencyMatrixSize = 4 * 16 * 128 * (vertexCount + 1);
    degreeMatrixSize = 4 * 16 * 1 * (vertexCount + 1);
    embeddingMatrixSize = 4 * 16 * 256 * (vertexCount + 1);
    networkXgraphsize = 60 * (vertexCount);

    long totalMemoryApproximation =
            (featureMatrixSize + adjacencyMatrixSize + degreeMatrixSize + embeddingMatrixSize + networkXgraphsize) /
            1024;

    return totalMemoryApproximation;


}

long JasmineGraphTrainingSchedular::getAvailableMemory(string hostname) {
    PerformanceSQLiteDBInterface refToPerfDb = *new PerformanceSQLiteDBInterface();
    refToPerfDb.init();
    string perfSqlStatement = "SELECT memory_usage FROM performance_data where ip_address = '" + hostname +
                              "' ORDER BY date_time DESC LIMIT 1";
    vector<vector<pair<string, string>>> result = refToPerfDb.runSelect(perfSqlStatement);
    if (result.size() == 0) {
        return 0;
    }
    long availableMemory = stol(result[0][0].second);
    return availableMemory;

}

std::map<int, int>
JasmineGraphTrainingSchedular::packPartitionsToMemory(vector<pair<int, int>> partitionMemoryList, int capacity) {

    std::map<int, int> partitionToIteration;

    int res = 0;
    int n = partitionMemoryList.size();
    int bin_rem[n];

    for (int i = 0; i < n; i++) {
        int j;
        int min = capacity + 1;
        int bestBin = 0;

        for (j = 0; j < res; j++) {
            if (bin_rem[j] >= partitionMemoryList[i].second && bin_rem[j] - partitionMemoryList[i].second < min) {
                bestBin = j;
                min = bin_rem[j] - partitionMemoryList[i].second;
            }
        }
        if (min == capacity + 1) {
            vector<int> partitionBin;
            bin_rem[res] = capacity - partitionMemoryList[i].second;
            partitionToIteration[partitionMemoryList[i].first] = res;
            res++;
        } else {
            bin_rem[bestBin] -= partitionMemoryList[i].second;
            partitionToIteration[partitionMemoryList[i].first] = bestBin;
        }
    }
    trainScheduler_logger.log("Packing partitions to fit memory completed", "info");
    return partitionToIteration;
}
