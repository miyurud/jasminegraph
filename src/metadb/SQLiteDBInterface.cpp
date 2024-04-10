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

#include "SQLiteDBInterface.h"

#include <string>

#include "../util/Utils.h"
#include "../util/logger/Logger.h"

using namespace std;
Logger db_logger;

SQLiteDBInterface::SQLiteDBInterface() {
    this->databaseLocation = Utils::getJasmineGraphProperty("org.jasminegraph.db.location");
}

SQLiteDBInterface::SQLiteDBInterface(string databaseLocation) {
    this->databaseLocation = databaseLocation;
}

int SQLiteDBInterface::init() {
    if (!Utils::fileExists(this->databaseLocation.c_str())) {
        if (Utils::createDatabaseFromDDL(this->databaseLocation.c_str(), ROOT_DIR "ddl/metadb.sql") != 0) {
            return -1;
        }
    }

    if (sqlite3_open(this->databaseLocation.c_str(), &database)) {
        db_logger.error("Cannot open database: " + string(sqlite3_errmsg(database)));
        return -1;
    }
    db_logger.info("Database opened successfully :" + this->databaseLocation);
    return 0;
}
