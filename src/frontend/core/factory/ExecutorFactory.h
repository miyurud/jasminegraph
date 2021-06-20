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

#ifndef JASMINEGRAPH_EXECUTORFACTORY_H
#define JASMINEGRAPH_EXECUTORFACTORY_H

#include <string>
#include "../domain/JobRequest.h"
#include "../executor/AbstractExecutor.h"
#include "../../../metadb/SQLiteDBInterface.h"
#include "../../../performancedb/PerformanceSQLiteDBInterface.h"
#include "../../JasmineGraphFrontEndProtocol.h"


class ExecutorFactory {
public:
    ExecutorFactory(SQLiteDBInterface db, PerformanceSQLiteDBInterface perfDb);
    AbstractExecutor* getExecutor(JobRequest jobRequest);

private:
    SQLiteDBInterface sqliteDB;
    PerformanceSQLiteDBInterface perfDB;
};


#endif //JASMINEGRAPH_EXECUTORFACTORY_H
