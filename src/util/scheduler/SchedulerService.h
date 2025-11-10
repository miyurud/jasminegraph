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

#ifndef JASMINEGRAPH_SCHEDULERSERVICE_H
#define JASMINEGRAPH_SCHEDULERSERVICE_H

#include <thread>

#include "../../performance/metrics/PerformanceUtil.h"
#include "../../performancedb/PerformanceSQLiteDBInterface.h"
#include "../Utils.h"
#include "../logger/Logger.h"

class SchedulerService {
 public:
    static void startScheduler();
    static void startPerformanceScheduler();
    static void startCumulativePerformanceMetricScheduler();
};

#endif  // JASMINEGRAPH_SCHEDULERSERVICE_H
