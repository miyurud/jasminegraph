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

#include "scaler.h"

#include <map>
#include <thread>
#include <vector>
#include <unistd.h>

#include "../metadb/SQLiteDBInterface.h"
#include "../util/Utils.h"

using namespace std;

std::mutex schedulerMutex;
static std::thread *scale_down_thread = nullptr;
static volatile bool running = false;
SQLiteDBInterface *sqlite = nullptr;

static void scale_down_thread_fn();

void start_scale_down(SQLiteDBInterface *sqliteInterface) {
    if (scale_down_thread) return;
    sqlite = sqliteInterface;
    running = true;
    scale_down_thread = new std::thread(scale_down_thread_fn);
}

void stop_scale_down() {
    if (!scale_down_thread) return;
    running = false;
    scale_down_thread->join();
    scale_down_thread = nullptr;
}

static void scale_down_thread_fn() {
    while (running) {
        sleep(30);
        if (!running) break;
        std::vector<string> workers;  // "ip:port"
        const std::vector<vector<pair<string, string>>> &results =
            sqlite->runSelect("SELECT DISTINCT idworker,ip,server_port FROM worker;");
        for (int i = 0; i < results.size(); i++) {
            string ip = results[i][1].second;
            string port = results[i][2].second;
            workers.push_back(ip + ":" + port);
        }

        map<string, int> loads;
        const map<string, string> &cpu_map = Utils::getMetricMap("cpu_usage");
        for (auto it = workers.begin(); it != workers.end(); it++) {
            auto &worker = *it;
            const auto workerLoadIt = cpu_map.find(worker);
            if (workerLoadIt != cpu_map.end()) {
                double load = stod(workerLoadIt->second.c_str());
                if (load < 0) load = 0;
                loads[worker] = (int)(4 * load);
            } else {
                loads[worker] = 0;
            }
        }
    }
}
