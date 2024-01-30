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

#ifndef JASMINEGRAPH_K8SWORKERCONTROLLER_H
#define JASMINEGRAPH_K8SWORKERCONTROLLER_H

#include <vector>

extern "C" {
#include <kubernetes/api/AppsV1API.h>
}

#include "../metadb/SQLiteDBInterface.h"
#include "./K8sInterface.h"

class K8sWorkerController {
 private:
    K8sInterface *interface;
    SQLiteDBInterface metadb;

    std::string masterIp;
    int numberOfWorkers;

    std::map<std::string, int> nodes;

    void spawnWorker(int workerId);

    void deleteWorker(int workerId);

    int attachExistingWorkers();

 public:
    K8sWorkerController(std::string masterIp, int numberOfWorkers, SQLiteDBInterface *metadb);

    ~K8sWorkerController();

    std::string getMasterIp() const;

    int getNumberOfWorkers() const;

    void setNumberOfWorkers(int newNumberOfWorkers);
};

#endif  // JASMINEGRAPH_K8SWORKERCONTROLLER_H
