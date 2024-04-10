/**
Copyright 2018 JasminGraph Team
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

#include "PerformanceSQLiteDBInterface.h"

#include <string>

#include "../util/Utils.h"
#include "../util/logger/Logger.h"

using namespace std;
Logger perfdb_logger;

int PerformanceSQLiteDBInterface::init() {
    if (!Utils::fileExists(this->databaseLocation.c_str())) {
        if (Utils::createDatabaseFromDDL(this->databaseLocation.c_str(), ROOT_DIR "ddl/perfdb.sql") != 0) {
            return -1;
        }
    }

    int rc =
        sqlite3_open(this->databaseLocation.c_str(), &database);
    if (rc) {
        perfdb_logger.log("Cannot open database: " + string(sqlite3_errmsg(database)), "error");
        return -1;
    }
    perfdb_logger.log("Database opened successfully", "info");
    return 0;
}

PerformanceSQLiteDBInterface::PerformanceSQLiteDBInterface() {
    this->databaseLocation = Utils::getJasmineGraphProperty("org.jasminegraph.performance.db.location");
}

PerformanceSQLiteDBInterface::PerformanceSQLiteDBInterface(string databaseLocation) {
    this->databaseLocation = databaseLocation;
}
