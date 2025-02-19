/**
Copyright 2024 JasmineGraph Team
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

#ifndef JASMINEGRAPH_SRC_UTIL_DBINTERFACE_H_
#define JASMINEGRAPH_SRC_UTIL_DBINTERFACE_H_

#include <string>
#include <vector>
#include <sqlite3.h>

using namespace std;

class DBInterface {
 protected:
    sqlite3 *database;
    std::string databaseLocation;

 public:
    virtual int init() = 0;

    int finalize();

    std::vector<std::vector<std::pair<std::string, std::string>>> runSelect(std::string);

    int runInsert(std::string);

    void runUpdate(std::string);

    void runInsertNoIDReturn(std::string);

    int runSqlNoCallback(const char *zSql);

    bool isGraphIdExist(std::string);

    int getNextGraphId();

    std::string getPartitionAlgoByGraphID(std::string graphID);
};

#endif  // JASMINEGRAPH_SRC_UTIL_DBINTERFACE_H_
