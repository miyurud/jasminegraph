/**
Copyright 2021 JasmineGraph Team
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

#include "ExecutorFactory.h"

#include "../executor/impl/TriangleCountExecutor.h"
#include "../executor/impl/StreamingTriangleCountExecutor.h"
#include "../executor/impl/PageRankExecutor.h"
#include "../executor/impl/CypherQueryExecutor.h"

ExecutorFactory::ExecutorFactory(SQLiteDBInterface *db, PerformanceSQLiteDBInterface *perfDb) {
    this->sqliteDB = db;
    this->perfDB = perfDb;
}

AbstractExecutor* ExecutorFactory::getExecutor(JobRequest jobRequest) {
    if (TRIANGLES == jobRequest.getJobType()) {
        return new TriangleCountExecutor(this->sqliteDB, this->perfDB, jobRequest);
    } else if (STREAMING_TRIANGLES == jobRequest.getJobType()) {
        return new StreamingTriangleCountExecutor(this->sqliteDB, jobRequest);
    } else if (PAGE_RANK == jobRequest.getJobType()) {
        return new PageRankExecutor(this->sqliteDB, this->perfDB, jobRequest);
    } else if (CYPHER == jobRequest.getJobType()) {
        return new CypherQueryExecutor(this->sqliteDB, this->perfDB, jobRequest);
    }
    return nullptr;
}
