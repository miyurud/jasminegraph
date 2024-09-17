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

#ifndef JASMINEGRAPHFRONTENDCOMMON_H
#define JASMINEGRAPHFRONTENDCOMMON_H

#include <map>
#include <thread>

#include "../../../metadb/SQLiteDBInterface.h"
#include "../../../query/algorithms/triangles/Triangles.h"

class JasmineGraphFrontEndCommon {
public:
    static bool graphExists(std::string basic_string, SQLiteDBInterface *sqlite);

    static bool graphExistsByID(std::string id, SQLiteDBInterface *sqlite);

    static void removeGraph(std::string graphID, SQLiteDBInterface *sqlite, std::string masterIP);

    static bool isGraphActive(string graphID, SQLiteDBInterface *sqlite);

    static bool modelExists(std::string basic_string, SQLiteDBInterface *sqlite);

    static bool modelExistsByID(std::string id, SQLiteDBInterface *sqlite);

    static void getAndUpdateUploadTime(std::string graphID, SQLiteDBInterface *sqlite);

    static bool isGraphActiveAndTrained(std::string graphID, SQLiteDBInterface *sqlite);

    static map<long, long> getOutDegreeDistributionHashMap(map<long, unordered_set<long>> graphMap);

    static int getUid();

    static long getSLAForGraphId(SQLiteDBInterface *sqlite, PerformanceSQLiteDBInterface *perfSqlite,
                             std::string graphId, std::string command, std::string category);

    // Method to execute SQL query and return results
    static std::vector<std::vector<std::pair<std::string, std::string>>> getGraphData(SQLiteDBInterface *sqlite);
};

#endif //JASMINEGRAPHFRONTENDCOMMON_H
