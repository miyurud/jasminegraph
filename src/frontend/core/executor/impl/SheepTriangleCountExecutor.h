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

#ifndef JASMINEGRAPH_SHEEPTRIANGLECOUNTEXECUTOR_H
#define JASMINEGRAPH_SHEEPTRIANGLECOUNTEXECUTOR_H

#include <chrono>
#include <unordered_map>
#include <unordered_set>

#include "../../../../metadb/SQLiteDBInterface.h"
#include "../../../../performance/metrics/PerformanceUtil.h"
#include "../../../../performancedb/PerformanceSQLiteDBInterface.h"
#include "../../../../server/JasmineGraphInstanceProtocol.h"
#include "../../../../server/JasmineGraphServer.h"
#include "../../../JasmineGraphFrontEndProtocol.h"
#include "../../CoreConstants.h"
#include "../AbstractExecutor.h"

/**
 * Dedicated executor for sheep-partitioned graph triangle counting.
 * This executor is specialized for graphs partitioned using the sheep algorithm
 * and uses optimized triangle counting methods.
 * 
 * Uses async-based threading strategy and delegates to TriangleCountExecutor's shared logic.
 */
class SheepTriangleCountExecutor : public AbstractExecutor {
 public:
    SheepTriangleCountExecutor();

    SheepTriangleCountExecutor(SQLiteDBInterface *db, PerformanceSQLiteDBInterface *perfDb, JobRequest jobRequest);

    void execute();

    int getUid();

 private:
    SQLiteDBInterface *sqlite;
    PerformanceSQLiteDBInterface *perfDB;
    JobRequest request;
};

#endif  // JASMINEGRAPH_SHEEPTRIANGLECOUNTEXECUTOR_H
