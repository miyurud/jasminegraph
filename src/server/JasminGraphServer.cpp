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

#include <iostream>
#include <map>
#include "JasminGraphServer.h"
#include "../metadb/SQLiteDBInterface.h"
#include "JasminGraphInstance.h"
#include "../util/Utils.h"

JasminGraphServer::JasminGraphServer()
{

}

int JasminGraphServer::run()
{
    std::cout << "Running the server..." << std::endl;
    sqlite3 *database;
    int rc = sqlite3_open("acacia_meta.db", &database);

    if (rc)
    {
        fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(database));
        return(-1);
    } else {
        fprintf(stderr, "Opened database successfully\n");
    }

    SQLiteDBInterface *sqlite = new SQLiteDBInterface();
    sqlite->init();
    init();

    sqlite3_close(database);
    return 0;
}

bool JasminGraphServer::isRunning() {
    return true;
}

void JasminGraphServer::init()
{
    Utils utils;
    std::map<string, string> map = utils.getBatchUploadFileList();
}
