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

#include "JasmineGraphTrainingSchedular.h"

#include <iostream>

#include "../../metadb/SQLiteDBInterface.h"
#include "../../performancedb/PerformanceSQLiteDBInterface.h"
#include "../../util/Conts.h"
#include "../../util/logger/Logger.h"
#include "algorithm"

using namespace std;
Logger trainScheduler_logger;

map<string, std::map<int, int>> JasmineGraphTrainingSchedular::schedulePartitionTraining(std::string graphID) {
    map<string, std::map<int, int>> scheduleForEachHost;
    vector<pair<string, string>> hostData;
    SQLiteDBInterface refToSqlite = *new SQLiteDBInterface();
    refToSqlite.init();

    string sqlStatement =
        "SELECT host_idhost, name FROM worker_has_partition INNER JOIN worker ON worker_idworker = "
        "idworker WHERE partition_graph_idgraph = " +
        graphID + " group by host_idhost";
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
        sqlStatement =
            "SELECT idpartition, vertexcount, central_vertexcount, graph_idgraph FROM partition INNER JOIN "
            "(SELECT host_idhost, partition_idpartition, partition_graph_idgraph, worker_idworker FROM "
            "worker_has_partition INNER JOIN worker ON worker_idworker = idworker) AS a ON partition.idpartition = "
            "a.partition_idpartition WHERE partition.graph_idgraph =  " +
            graphID + " AND a.host_idhost = " + j->first;
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
            memoryEstimationForEachPartition.push_back(make_pair(i->second, (int)memoryForPartition));
        }
        std::map<int, int> scheduledPartitionSets =
            packPartitionsToMemory(memoryEstimationForEachPartition, availableMemory);
        scheduleForEachHost.insert(make_pair(j->second, scheduledPartitionSets));
    }
    return scheduleForEachHost;
}

long JasmineGraphTrainingSchedular::estimateMemory(int vertexCount, string graph_id) {
    SQLiteDBInterface refToSqlite = *new SQLiteDBInterface();
    refToSqlite.init();

    string sqlStatement = "SELECT feature_count FROM graph WHERE idgraph = " + graph_id;
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
        (featureMatrixSize + adjacencyMatrixSize + degreeMatrixSize + embeddingMatrixSize + networkXgraphsize) / 1024;

    return totalMemoryApproximation;
}

long JasmineGraphTrainingSchedular::getAvailableMemory(string hostname) {
    PerformanceSQLiteDBInterface refToPerfDb = *new PerformanceSQLiteDBInterface();
    refToPerfDb.init();
    trainScheduler_logger.log("Fetching available host " + hostname + " memory", "info");
    string perfSqlStatement =
        "SELECT memory_usage FROM host_performance_data INNER JOIN (SELECT idhost FROM host WHERE ip = '" + hostname +
        "') USING (idhost) ORDER BY date_time DESC LIMIT 1";
    vector<vector<pair<string, string>>> result = refToPerfDb.runSelect(perfSqlStatement);
    if (result.size() == 0) {
        return 0;
    }
    long availableMemory = stol(result[0][0].second);
    return availableMemory;
}

std::map<int, int> JasmineGraphTrainingSchedular::packPartitionsToMemory(vector<pair<int, int>> partitionMemoryList,
                                                                         int capacity) {
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
 *  @return Map of host to maps containing schedule for that host given by schedulePartitionsBestFit method
 */
map<string, std::map<int, map<int, int>>> JasmineGraphTrainingSchedular::scheduleGradientPassingTraining(
    std::string graphID) {
    map<string, map<int, map<int, int>>> scheduleForEachHost;
    vector<pair<string, string>> hostData;
    SQLiteDBInterface refToSqlite = *new SQLiteDBInterface();
    refToSqlite.init();

    // Get graph attribute metadata
    string sql = "SELECT feature_count, feature_type FROM graph WHERE idgraph = " + graphID;
    std::vector<vector<pair<string, string>>> graphResult = refToSqlite.runSelect(sql);

    int featurecount = stoi(graphResult[0][0].second);  // Graph feature count

    // Set attribute value size in bytes based on attribute data type
    string attrType = graphResult[0][1].second;
    int featureSize;
    if (attrType == "int8")
        featureSize = 8;
    else if (attrType == "int16")
        featureSize = 16;
    else if (attrType == "int32")
        featureSize = 32;
    else if (attrType == "float")
        featureSize = 32;

    // Get details of hosts
    string sqlStatement =
        "SELECT host_idhost, name FROM worker_has_partition INNER JOIN worker ON worker_idworker = "
        "idworker WHERE partition_graph_idgraph = " +
        graphID + " group by host_idhost";
    std::vector<vector<pair<string, string>>> result = refToSqlite.runSelect(sqlStatement);
    // Store host details in vector
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

    trainScheduler_logger.log("Scheduling training order for each host", "info");

    // For each host, prepare a schedule
    for (std::vector<pair<string, string>>::iterator j = (hostData.begin()); j != hostData.end(); ++j) {
        // Get partitions and partition details for partitions in host
        sqlStatement =
            "SELECT idpartition, vertexcount, central_vertexcount, edgecount, central_edgecount, worker_idworker FROM "
            "partition INNER JOIN "
            "(SELECT host_idhost, partition_idpartition, partition_graph_idgraph, worker_idworker FROM "
            "worker_has_partition INNER JOIN worker ON worker_idworker = idworker) AS a ON partition.idpartition = "
            "a.partition_idpartition WHERE partition.graph_idgraph =  " +
            graphID + " AND a.host_idhost = " + j->first;
        std::vector<vector<pair<string, string>>> results = refToSqlite.runSelect(sqlStatement);

        vector<vector<int>> partitionMetadata;  // Vector for node count, edge count, feature count for memory
                                                // estimation of each partition
        map<int, int> partitionWorkerMap;
        // Iterate through partition list and get partition details
        trainScheduler_logger.log("Commence schedule creation for host" + j->second, "info");
        for (std::vector<vector<pair<string, string>>>::iterator i = results.begin(); i != results.end(); ++i) {
            std::vector<pair<string, string>> rowData = *i;
            id_partition = rowData.at(0).second;
            vertexCount = rowData.at(1).second;
            centralvertexCount = rowData.at(2).second;
            edgeCount = rowData.at(3).second;
            centralEdgeCount = rowData.at(4).second;

            // Compute partition metrics
            partitionID = stoi(id_partition);
            vertexcount = stoi(vertexCount) + stoi(centralvertexCount);
            edgecount = stoi(edgeCount) + stoi(centralEdgeCount);

            vector<int> partitionValues;  // Vector for partition node count, edge count and feature count
            partitionValues.push_back(partitionID);
            partitionValues.push_back(vertexcount);
            partitionValues.push_back(edgecount);
            partitionValues.push_back(featurecount);
            partitionValues.push_back(featureSize);

            partitionMetadata.push_back(partitionValues);

            // Store assigned worker per partition
            workerid = stoi(rowData.at(5).second);
            partitionWorkerMap[partitionID] = workerid;
        }

        long availableMemory = getAvailableMemory(j->second);  // Host memory (in KB)

        // Get memory estimation list for each partition of host
        vector<pair<int, double>> partitionMemoryList = estimateMemoryDistOpt(partitionMetadata, availableMemory);

        // Get schedule for host
        map<int, map<int, int>> scheduledPartitionSets =
            schedulePartitionsBestFit(partitionMemoryList, partitionWorkerMap, availableMemory);
        scheduleForEachHost.insert(make_pair(j->second, scheduledPartitionSets));
    }
    return scheduleForEachHost;
}

/** Estimate memory of graph partitions using the partition details
 *
 * @param partitionMetadata a vector of vectors containing partition ID, node count, edge count, feature count, feature
 * size (in bytes) per partition
 * @param availableMemory total memory of host (in KB)
 * @return Vector of pairs containing partition ids and estimated memory (in KB) for distributed opt training process
 */
vector<pair<int, double>> JasmineGraphTrainingSchedular::estimateMemoryDistOpt(vector<vector<int>> partitionMetadata,
                                                                               long availableMemory) {
    vector<pair<int, double>> partitionMemoryList;  // Vector of estimated sizes of partitions

    trainScheduler_logger.log("Estimating host partition size in memory", "info");
    // Iterate through partitions and estimate sizes
    for (vector<vector<int>>::iterator i = partitionMetadata.begin(); i != partitionMetadata.end(); i++) {
        int partitionID = (*i)[0];
        int nodeCount = (*i)[1];
        int edgeCount = (*i)[2];
        int featureCount = (*i)[3];
        int featureSize = (*i)[4];

        double graphSize =
            ((double)featureSize) * nodeCount * edgeCount * featureCount / (1024 * 1024);  // Size in hard disk (KB)

        // The following linear relationship was observed between the computed graph size and graph size in memory in
        // the experiments
        double sizeInMem = 3.6 * graphSize + 2 * 1024;  // Size when loaded into memory

        if (sizeInMem > availableMemory) {
            sizeInMem = availableMemory;
        }

        partitionMemoryList.emplace_back(partitionID, sizeInMem);
    }

    return partitionMemoryList;
}

/** Create schedule for host by best fitting in sorted order (descending)
 *
 * @param partitionMemoryList Vector of pairs containing partition ID and estimated size in memory
 * @param partitionWorkerMap Map of partition id and the worker assigned to partition
 * @param capacity total memory of host
 * @return Map from worker to maps from partition to order of loading into memory
 */
map<int, map<int, int>> JasmineGraphTrainingSchedular::schedulePartitionsBestFit(
    vector<pair<int, double>> partitionMemoryList, map<int, int> partitionWorkerMap, int capacity) {
    std::map<int, map<int, int>> schedule;  // Host schedule per worker and what partitions to load in which order
    cout << "Host memory " << capacity << endl;
    // Initialize the state of host workers as free
    std::map<int, bool> workerState;
    for (auto i : partitionWorkerMap) workerState[i.second] = true;
    cout << "No of workers in host " << workerState.size() << endl;

    // Sort partition memory list in descending. order
    sort(partitionMemoryList.begin(), partitionMemoryList.end(),
         [](pair<int, double> &a, pair<int, double> &b) { return (a.second > b.second); });
    cout << "No of graph partitions in host " << partitionMemoryList.size() << endl;

    int order = 0;                           // Order of partitions being loaded into memory
    double remainingMem = (double)capacity;  // Total remaining mem after each pass (iteration) in simulation
    vector<pair<int, double>> simulatedMem;  // Partitions in mem and their remaining work (in memory) in simulation

    trainScheduler_logger.log("Fitting partitions into memory for host", "info");
    while (schedule.size() < partitionMemoryList.size()) {
        int proceedToNextIter = 0;
        // Iterate through sorted partition list and assign to memory (best fit)
        for (vector<pair<int, double>>::iterator i = partitionMemoryList.begin(); i != partitionMemoryList.end(); i++) {
            int partition = i->first;
            double mem = i->second;
            int worker = partitionWorkerMap[partition];

            // Check that partition is not already scheduled
            if (schedule.find(worker) != schedule.end())
                continue;
            else if (schedule[worker].find(partition) != schedule[worker].end())
                continue;

            // Add to training order if partition smaller than remaining memory
            if (mem < remainingMem && workerState[worker]) {
                // Add partition to worker of host under current order
                schedule[worker][partition] = order;
                proceedToNextIter = 1;
                simulatedMem.emplace_back(partition, mem);
                // Change worker state to busy
                workerState[worker] = false;
            }
        }

        // Update order
        order += proceedToNextIter;

        // Get smallest left memory to train in a partition
        double minRemainingWork =
            min_element(simulatedMem.begin(), simulatedMem.end(), [](pair<int, int> lhs, pair<int, int> rhs) {
                return lhs.second < rhs.second;
            })->second;

        // Simulate that smallest partition in current simulated memory is finished
        for (vector<pair<int, double>>::iterator i = simulatedMem.begin(); i != simulatedMem.end(); i++) {
            int partition = i->first;
            double remainingWork = i->second;
            int worker = partitionWorkerMap[partition];
            if (remainingWork <= minRemainingWork) {
                // Free memory taken by partition
                for (vector<pair<int, double>>::iterator j = partitionMemoryList.begin();
                     j != partitionMemoryList.end(); j++) {
                    int p = i->first;
                    int m = i->second;
                    if (partition == p) remainingMem += m;
                }
                // Change worker state to free
                workerState[worker] = true;
                // Remove partition entry from simulated memory and update iterator pointer
                i = simulatedMem.erase(i) - 1;
            }
        }
    }

    return schedule;
}
