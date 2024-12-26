/**
Copyright 2019 JasminGraph Team
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

#include "JasmineGraphFrontendCommon.h"
#include "../../JasmineGraphFrontEndProtocol.h"
#include "../../../server/JasmineGraphServer.h"
#include "../../../util/logger/Logger.h"

Logger common_logger;

/**
 * This method checks if a graph exists in JasmineGraph.
 * This method uses the unique path of the graph.
 * @param basic_string
 * @param dummyPt
 * @return
 */
bool JasmineGraphFrontEndCommon::graphExists(string path, SQLiteDBInterface *sqlite) {
    string stmt = "SELECT COUNT( * ) FROM graph WHERE upload_path LIKE '" + path +
                  "' AND graph_status_idgraph_status = '" + to_string(Conts::GRAPH_STATUS::OPERATIONAL) + "';";
    std::vector<vector<pair<string, string>>> v = sqlite->runSelect(stmt);
    return (std::stoi(v[0][0].second) != 0);
}

/**
 * This method checks if an accessible graph exists in JasmineGraph with the same unique ID.
 * @param id
 * @param dummyPt
 * @return
 */
bool JasmineGraphFrontEndCommon::graphExistsByID(string id, SQLiteDBInterface *sqlite) {
    string stmt = "SELECT COUNT( * ) FROM graph WHERE idgraph = " + id;
    std::vector<vector<pair<string, string>>> v = sqlite->runSelect(stmt);
    return (std::stoi(v[0][0].second) != 0);
}

/**
 * This method removes a graph from JasmineGraph
 */
void JasmineGraphFrontEndCommon::removeGraph(std::string graphID, SQLiteDBInterface *sqlite, std::string masterIP) {
    vector<pair<string, string>> hostHasPartition;
    vector<vector<pair<string, string>>> hostPartitionResults = sqlite->runSelect(
        "SELECT name, partition_idpartition FROM worker_has_partition INNER JOIN worker ON "
        "worker_has_partition.worker_idworker = worker.idworker WHERE partition_graph_idgraph = " +
        graphID + ";");
    for (vector<vector<pair<string, string>>>::iterator i = hostPartitionResults.begin();
         i != hostPartitionResults.end(); ++i) {
        int count = 0;
        string hostname;
        string partitionID;
        for (std::vector<pair<string, string>>::iterator j = (i->begin()); j != i->end(); ++j) {
            if (count == 0) {
                hostname = j->second;
            } else {
                partitionID = j->second;
                hostHasPartition.push_back(pair<string, string>(hostname, partitionID));
            }
            count++;
        }
    }
    for (std::vector<pair<string, string>>::iterator j = (hostHasPartition.begin()); j != hostHasPartition.end(); ++j) {
        common_logger.info("HOST ID : " + j->second + " PARTITION ID : " + j->first);
    }
    sqlite->runUpdate("UPDATE graph SET graph_status_idgraph_status = " + to_string(Conts::GRAPH_STATUS::DELETING) +
                      " WHERE idgraph = " + graphID);

    JasmineGraphServer::removeGraph(hostHasPartition, graphID, masterIP);

    sqlite->runUpdate("DELETE FROM worker_has_partition WHERE partition_graph_idgraph = " + graphID);
    sqlite->runUpdate("DELETE FROM partition WHERE graph_idgraph = " + graphID);
    sqlite->runUpdate("DELETE FROM graph WHERE idgraph = " + graphID);
}

/**
 * This method checks whether the graph is active and trained
 * @param graphID
 * @param dummyPt
 * @return
 */
bool JasmineGraphFrontEndCommon::isGraphActiveAndTrained(std::string graphID, SQLiteDBInterface *sqlite) {
    string stmt = "SELECT COUNT( * ) FROM graph WHERE idgraph LIKE '" + graphID +
                  "' AND graph_status_idgraph_status = '" + to_string(Conts::GRAPH_STATUS::OPERATIONAL) +
                  "' AND train_status = '" + (Conts::TRAIN_STATUS::TRAINED) + "';";
    std::vector<vector<pair<string, string>>> v = sqlite->runSelect(stmt);
    return (std::stoi(v[0][0].second) != 0);
}

/**
 * This method checks whether the graph is active
 * @param graphID
 * @param dummyPt
 * @return
 */
bool JasmineGraphFrontEndCommon::isGraphActive(std::string graphID, SQLiteDBInterface *sqlite) {
    string stmt = "SELECT COUNT( * ) FROM graph WHERE idgraph LIKE '" + graphID +
                  "' AND graph_status_idgraph_status = '" + to_string(Conts::GRAPH_STATUS::OPERATIONAL) + "';";
    std::vector<vector<pair<string, string>>> v = sqlite->runSelect(stmt);
    return (std::stoi(v[0][0].second) != 0);
}

bool JasmineGraphFrontEndCommon::modelExistsByID(string id, SQLiteDBInterface *sqlite) {
    string stmt = "SELECT COUNT( * ) FROM model WHERE idmodel = " + id +
                  " and model_status_idmodel_status = " + to_string(Conts::GRAPH_STATUS::OPERATIONAL);
    std::vector<vector<pair<string, string>>> v = sqlite->runSelect(stmt);
    return (std::stoi(v[0][0].second) != 0);
}

bool JasmineGraphFrontEndCommon::modelExists(string path, SQLiteDBInterface *sqlite) {
    string stmt = "SELECT COUNT( * ) FROM model WHERE upload_path LIKE '" + path +
                  "' AND model_status_idmodel_status = '" + to_string(Conts::GRAPH_STATUS::OPERATIONAL) + "';";
    std::vector<vector<pair<string, string>>> v = sqlite->runSelect(stmt);
    return (std::stoi(v[0][0].second) != 0);
}

void JasmineGraphFrontEndCommon::getAndUpdateUploadTime(std::string graphID, SQLiteDBInterface *sqlite) {
    struct tm tm;
    vector<vector<pair<string, string>>> uploadStartFinishTimes =
        sqlite->runSelect("SELECT upload_start_time,upload_end_time FROM graph WHERE idgraph = '" + graphID + "'");
    string startTime = uploadStartFinishTimes[0][0].second;
    string endTime = uploadStartFinishTimes[0][1].second;
    string sTime = startTime.substr(startTime.size() - 14, startTime.size() - 5);
    string eTime = endTime.substr(startTime.size() - 14, startTime.size() - 5);
    strptime(sTime.c_str(), "%H:%M:%S", &tm);
    time_t start = mktime(&tm);
    strptime(eTime.c_str(), "%H:%M:%S", &tm);
    time_t end = mktime(&tm);
    double difTime = difftime(end, start);
    sqlite->runUpdate("UPDATE graph SET upload_time = " + to_string(difTime) + " WHERE idgraph = " + graphID);
    common_logger.info("Upload time updated in the database");
}

map<long, long> JasmineGraphFrontEndCommon::getOutDegreeDistributionHashMap(map<long, unordered_set<long>> graphMap) {
    map<long, long> distributionHashMap;

    for (map<long, unordered_set<long>>::iterator it = graphMap.begin(); it != graphMap.end(); ++it) {
        long distribution = (it->second).size();
        distributionHashMap[it->first] = distribution;
    }
    return distributionHashMap;
}

int JasmineGraphFrontEndCommon::getUid() {
    static std::atomic<std::uint32_t> uid{0};
    return ++uid;
}

long JasmineGraphFrontEndCommon::getSLAForGraphId(SQLiteDBInterface *sqlite, PerformanceSQLiteDBInterface *perfSqlite,
                                            std::string graphId, std::string command, std::string category) {
    long graphSLAValue = 0;

    string sqlStatement =
        "SELECT worker_idworker, name,ip,user,server_port,server_data_port,partition_idpartition "
        "FROM worker_has_partition INNER JOIN worker ON worker_has_partition.worker_idworker=worker.idworker "
        "WHERE partition_graph_idgraph=" +
        graphId + ";";

    std::vector<vector<pair<string, string>>> results = sqlite->runSelect(sqlStatement);
    int partitionCount = results.size();

    string graphSlaQuery =
        "select graph_sla.sla_value from graph_sla,sla_category where graph_sla.id_sla_category=sla_category.id "
        "and sla_category.command='" +
        command + "' and sla_category.category='" + category +
        "' and "
        "graph_sla.graph_id='" +
        graphId + "' and graph_sla.partition_count='" + std::to_string(partitionCount) + "';";

    std::vector<vector<pair<string, string>>> slaResults = perfSqlite->runSelect(graphSlaQuery);

    if (slaResults.size() > 0) {
        string currentSlaString = slaResults[0][0].second;
        long graphSLAValue = atol(currentSlaString.c_str());
    }

    return graphSLAValue;
}

std::vector<std::vector<std::pair<std::string, std::string>>>
    JasmineGraphFrontEndCommon::getGraphData(SQLiteDBInterface *sqlite) {
    return sqlite->runSelect("SELECT idgraph, name, upload_path, graph_status_idgraph_status, vertexcount, edgecount, centralpartitioncount FROM graph;");
}

bool JasmineGraphFrontEndCommon::checkServerBusy(std::atomic<int> *currentFESession, int connFd) {
    if (*currentFESession >= Conts::MAX_FE_SESSIONS) {
        if (!Utils::send_str_wrapper(connFd, "JasmineGraph server is busy. Please try again later.")) {
            common_logger.error("Error writing to socket");
        }
        close(connFd);
        return true;
    }
    (*currentFESession)++;  // Increment only if not busy
    return false;
}

std::string JasmineGraphFrontEndCommon::readAndProcessInput(int connFd, char* data, int &failCnt) {
    std::string line = Utils::read_str_wrapper(connFd, data, FRONTEND_DATA_LENGTH, true);
    if (line.empty()) {
        failCnt++;
        if (failCnt > 4) {
            return "";
        }
        sleep(1);
    } else {
        failCnt = 0;
    }
    return Utils::trim_copy(line);
}

std::string JasmineGraphFrontEndCommon::getPartitionCount(std::string path) {
    if (Utils::getJasmineGraphProperty("org.jasminegraph.autopartition.enabled") != "true") {
        return "";
    }
    ifstream dataFile(path);
    size_t edges = std::count(std::istreambuf_iterator<char>(dataFile), std::istreambuf_iterator<char>(), '\n');
    dataFile.close();
    int partCnt = (int)round(pow(edges, 0.2) / 6);
    if (partCnt < 2) partCnt = 2;
    return to_string(partCnt);
}
