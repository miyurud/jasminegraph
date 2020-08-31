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

#include "algorithm"
#include "JasmineGraphTrainingSchedular.h"
#include "../../metadb/SQLiteDBInterface.h"
#include "../../util/Conts.h"
#include "../../performancedb/PerformanceSQLiteDBInterface.h"
#include "../../util/logger/Logger.h"

using namespace std;
Logger trainScheduler_logger;

map<string, std::map<int, int>> JasmineGraphTrainingSchedular::schedulePartitionTraining(std::string graphID) {

    map<string, std::map<int, int>> scheduleForEachHost;
    vector<pair<string, string>> hostData;
    SQLiteDBInterface refToSqlite = *new SQLiteDBInterface();
    refToSqlite.init();

    string sqlStatement = "SELECT host_idhost, name FROM worker_has_partition INNER JOIN worker ON worker_idworker = "
                          "idworker WHERE partition_graph_idgraph = " + graphID + " group by host_idhost";
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
    trainScheduler_logger.log("Scheduling training order for each worker", "info");
    for (std::vector<pair<string, string>>::iterator j = (hostData.begin()); j != hostData.end(); ++j) {
        sqlStatement = "SELECT idpartition, vertexcount, central_vertexcount, graph_idgraph FROM partition INNER JOIN "
                       "(SELECT host_idhost, partition_idpartition, partition_graph_idgraph, worker_idworker FROM "
                       "worker_has_partition INNER JOIN worker ON worker_idworker = idworker) AS a ON partition.idpartition = "
                       "a.partition_idpartition WHERE partition.graph_idgraph =  "+ graphID +" AND a.host_idhost = " + j->first;
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
        int min = capacity + 1;
        int bestBin = 0;

        for (int j = 0; j < res; j++) {
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

/** Method to initiate the creation of training schedule
 *
 *  @param graphID ID of graph to be trained
 */
map<string, std::map<int, map<int, int>>> JasmineGraphTrainingSchedular::scheduleGradientPassingTraining(std::string graphID) {

    map<string, map<int, map<int, int>>> scheduleForEachHost;
    vector<pair<string, string>> hostData;
    SQLiteDBInterface refToSqlite = *new SQLiteDBInterface();
    refToSqlite.init();

    //Get feature count of graph
    string sql = "SELECT feature_count FROM graph WHERE idgraph = " + graphID;
    std::vector<vector<pair<string, string>>> graphResult = refToSqlite.runSelect(sql);
    int featurecount = stoi(graphResult[0][0].second);

    //Get details of hosts
    string sqlStatement = "SELECT host_idhost, name FROM worker_has_partition INNER JOIN worker ON worker_idworker = "
                          "idworker WHERE partition_graph_idgraph = " + graphID + " group by host_idhost";
    std::vector<vector<pair<string, string>>> result = refToSqlite.runSelect(sqlStatement);
    //Store host details in vector
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
    string edgeCount = "";
    string centralEdgeCount = "";
    int partitionID;
    int vertexcount;
    int edgecount;
    int workerid;

    trainScheduler_logger.log("Scheduling training order for each worker", "info");

    //For each host, prepare a schedule
    for (std::vector<pair<string, string>>::iterator j = (hostData.begin()); j != hostData.end(); ++j) {
        //Get partitions and partition details for partitions in host
        sqlStatement = "SELECT idpartition, vertexcount, central_vertexcount, edgecount, central_edgecount, worker_idworker FROM partition INNER JOIN "
                       "(SELECT host_idhost, partition_idpartition, partition_graph_idgraph, worker_idworker FROM "
                       "worker_has_partition INNER JOIN worker ON worker_idworker = idworker) AS a ON partition.idpartition = "
                       "a.partition_idpartition WHERE partition.graph_idgraph =  " + graphID + " AND a.host_idhost = " +
                       j->first;
        std::vector<vector<pair<string, string>>> results = refToSqlite.runSelect(sqlStatement);

        vector<vector<int>> partitionDetails; //Vector for node count, edge count, feature count for memory estimation of each partition
        map<int, int> partitionWorkerMap;
        //Iterate through partition list and get partition details
        for (std::vector<vector<pair<string, string>>>::iterator i = results.begin(); i != results.end(); ++i) {
            std::vector<pair<string, string>> rowData = *i;
            id_partition = rowData.at(0).second;
            vertexCount = rowData.at(1).second;
            centralvertexCount = rowData.at(2).second;
            edgeCount = rowData.at(3).second;
            centralEdgeCount = rowData.at(4).second;

            //Compute partition metrics
            partitionID = stoi(id_partition);
            vertexcount = stoi(vertexCount) + stoi(centralvertexCount);
            edgecount = stoi(edgeCount) + stoi(centralEdgeCount);

            vector<int> partitionCounts; //Vector for partition node count, edge count and feature count
            partitionCounts.push_back(partitionID);
            partitionCounts.push_back(vertexcount);
            partitionCounts.push_back(edgecount);
            partitionCounts.push_back(featurecount);

            partitionDetails.push_back(partitionCounts);

            //Store assigned worker per partition
            workerid = stoi(rowData.at(5).second);
            partitionWorkerMap[partitionID] = workerid;

        }

        long availableMemory = getAvailableMemory(j->second); //Host memory

        //Get memory estimation list for each partition of host
        vector<pair<int, int>> partitionMemoryList = estimateMemoryDistOpt(partitionDetails, availableMemory);

        //Get schedule for host
        map<int, map<int, int>> scheduledPartitionSets = bestFitPartition(partitionMemoryList, partitionWorkerMap, availableMemory);
        scheduleForEachHost.insert(make_pair(j->second, scheduledPartitionSets));
    }
    return scheduleForEachHost;
}

/** Estimate memory of graph partitions using the partition details
 *
 * @param partitionDetails a vector of vectors containing partition ID, node count, edge count, feature count per partition
 * @param availableMemory total memory of host
 */
vector<pair<int, int>>
JasmineGraphTrainingSchedular::estimateMemoryDistOpt(vector<vector<int>> partitionDetails, long availableMemory) {


    vector<pair<int, int>> partitionMemoryList; //Vector of estimated sizes of partitions

    //Iterate through partitions and estimate sizes
    for (vector<vector<int>>::iterator i = partitionDetails.begin(); i != partitionDetails.end(); i++) {
        int partitionID = (*i)[0];
        int nodeCount = (*i)[1];
        int edgeCount = (*i)[2];
        int featureCount = (*i)[3];

        int graphSize = 8*nodeCount*edgeCount*featureCount / (1024 * 1024); //Size in hard disk

        int sizeInMem = 36 * graphSize + 2; //Size when loaded into memory

        if (sizeInMem > availableMemory) {
            sizeInMem = availableMemory;
        }

        partitionMemoryList.push_back(make_pair(partitionID, sizeInMem));
    }

    return partitionMemoryList;
}

/** Create schedule for host by best fitting in sorted order (descending)
 *
 * @param partitionMemoryList Vector of pairs containing partition ID and estimated size in memory
 * @param partitionWorkerMap Map of partition id and the worker assigned to partition
 * @param capacity total memory of host
 */
std::map<int, map<int, int>>
JasmineGraphTrainingSchedular::bestFitPartition(vector<pair<int, int>> partitionMemoryList, map<int, int> partitionWorkerMap, int capacity) {

    std::map<int, map<int, int>> schedule; //Host schedule per worker and what partitions to load in which order

    //Initialize the state of host workers as free
    std::map<int, bool> workerState;
    for (auto i: partitionWorkerMap) workerState[i.second] = true;

    //Sort partition memory list in descending order
    sort(partitionMemoryList.begin(), partitionMemoryList.end(),
         [](pair<int, int> &a, pair<int, int> &b) { return (a.second > b.second); });


    int order = 0; //Order of partitions being loaded into memory
    int remainingMem = capacity; //Total remaining mem after each pass (iteration) in simulation
    vector<pair<int, int>> simulatedMem; //Partitions in mem and their remaining work (in memory) in simulation

    while (schedule.size() < partitionMemoryList.size()) {
        int proceedToNextIter = 0;
        //Iterate through sorted partition list and assign to memory (best fit)
        for (vector<pair<int, int>>::iterator i = partitionMemoryList.begin(); i != partitionMemoryList.end(); i++) {
            int partition = i->first;
            int mem = i->second;
            int worker = partitionWorkerMap[partition];

            //Check that partition is not already scheduled
            if (schedule.find(partition) == schedule.end()) continue;

            //Add to training order if partition smaller than remaining memory
            if (mem < remainingMem && workerState[worker]) {
                //Add partition to worker of host under current order
                schedule[worker][partition] = order;
                proceedToNextIter = 1;
                simulatedMem.push_back(make_pair(partition, mem));
                //Change worker state to busy
                workerState[worker] = false;
            }
        }

        //Update order
        order += proceedToNextIter;

        //Get smallest left memory to train in a partition
        int minRemainingWork = min_element(simulatedMem.begin(), simulatedMem.end(),
                                           [](pair<int, int> lhs, pair<int, int> rhs) {
                                               return lhs.second < rhs.second;
                                           })->second;

        //Simulate that smallest partition in current simulated memory is finished
        for (vector<pair<int, int>>::iterator i = simulatedMem.begin(); i != simulatedMem.end(); i++) {
            int partition = i->first;
            int remainingWork = i->second;
            int worker = partitionWorkerMap[partition];
            if (remainingWork <= minRemainingWork) {
                //Free memory taken by partition
                for (vector<pair<int, int>>::iterator j = partitionMemoryList.begin();
                     j != partitionMemoryList.end(); j++) {
                    int p = i->first;
                    int m = i->second;
                    if (partition == p) remainingMem += m;
                }
                //Change worker state to free
                workerState[worker] = true;
                //Remove partition entry from simulated memory
                simulatedMem.erase(i);
            }
        }
    }

    return schedule;
}

