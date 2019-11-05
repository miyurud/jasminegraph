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


using namespace std;

void JasmineGraphTrainingSchedular::sortPartions(std::string graphID) {

    vector<pair<string, string>> hostData;
    SQLiteDBInterface refToSqlite = *new SQLiteDBInterface();
    refToSqlite.init();

    string sqlStatement =
            "SELECT idhost, name FROM host_has_partition INNER JOIN host ON host_idhost = idhost WHERE partition_graph_idgraph = '" +
            to_string(1) + "' group by idhost";
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

    for (std::vector<pair<string, string>>::iterator j = (hostData.begin()); j != hostData.end(); ++j) {
        sqlStatement =
                "SELECT idpartition,vertexcount,central_vertexcount,graph_idgraph FROM partition INNER JOIN host_has_partition "
                "ON partition.idpartition = host_has_partition.partition_idpartition WHERE graph_idgraph = " + graphID +
                " AND host_idhost = "
                + j->first;
        std::vector<vector<pair<string, string>>> results = refToSqlite.runSelect(sqlStatement);

        std::map<int, int> vertexCountToPartitionId;
        cout << sqlStatement << endl;
        for (std::vector<vector<pair<string, string>>>::iterator i = results.begin(); i != results.end(); ++i) {
            std::vector<pair<string, string>> rowData = *i;

            id_partition = rowData.at(0).second;
            vertexCount = rowData.at(1).second;
            centralvertexCount = rowData.at(2).second;

            partition_id = stoi(id_partition);
            vertexcount = stoi(vertexCount);
            centralVertexCount = stoi(centralvertexCount);

            //ToDO:get triangle count by partition id

            vertexCountToPartitionId.insert({vertexcount + centralVertexCount, partition_id});


        }

        float totMemEstimation = 0;
        int iteration = 0;
        for (auto i = vertexCountToPartitionId.begin(); i != vertexCountToPartitionId.end(); ++i) {
            totMemEstimation += estimateMemory(i->first);
            if (totMemEstimation < 600000000) {  //total=65843492
                iteration++;
                partitionSequence[j->second].push_back(i->second);
            } else {
                partitionSequenceIteration[j->second].push_back(iteration);
                iteration = 0;
                totMemEstimation = 0;
                partitionSequence[j->second].push_back(i->second);
                iteration++;
                totMemEstimation += estimateMemory(i->first);
            }
        }
    }


}

float JasmineGraphTrainingSchedular::estimateMemory(int vertexCount) {
    float featureMatrixSize;
    float adjacencyMatrixSize;
    float degreeMatrixSize;
    float embeddingMatrixSize;
    int featureCount = 100;
    float networkXgraphsize;

    featureMatrixSize = 4 * 16 * featureCount * (vertexCount + 1);
    adjacencyMatrixSize = 4 * 16 * 128 * (vertexCount + 1);
    degreeMatrixSize = 4 * 16 * 1 * (vertexCount + 1);
    embeddingMatrixSize = 4 * 16 * 256 * (vertexCount + 1);
    networkXgraphsize = 60 * (vertexCount);

    float totalMemoryApproximation =
            (featureMatrixSize + adjacencyMatrixSize + degreeMatrixSize + embeddingMatrixSize + networkXgraphsize) /
            1024;

    return totalMemoryApproximation;

}


std::map<std::string, std::vector<int>> JasmineGraphTrainingSchedular::getScheduledPartitionList() {
    return partitionSequence;
}


std::map<std::string, std::vector<int>> JasmineGraphTrainingSchedular::getSchduledIteratorList() {
    return partitionSequenceIteration;

}