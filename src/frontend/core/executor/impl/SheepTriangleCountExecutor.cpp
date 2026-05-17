/**
Copyright 2026 JasmineGraph Team
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

#include "SheepTriangleCountExecutor.h"
#include "TriangleCountExecutor.h"

Logger sheepTriangleCount_logger;

// SheepTriangleCountExecutor::SheepTriangleCountExecutor() = default;

SheepTriangleCountExecutor::SheepTriangleCountExecutor(SQLiteDBInterface *db, PerformanceSQLiteDBInterface *perfDb,
                                                       const JobRequest& jobRequest)
    : sqlite(db), perfDB(perfDb), request(jobRequest) {
}

int SheepTriangleCountExecutor::getUid() const {
    static int counter = 0;
    return counter++;
}

void SheepTriangleCountExecutor::execute() {
    TriangleCountExecutor::executeTriangleCount(sqlite, perfDB, request,
                                                 TriangleCountCommandType::SHEEP_TRIANGLES,
                                                 ThreadingStrategy::ASYNC_BASED, sheepTriangleCount_logger);
}
