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

#include <unistd.h>

#include <map>
#include <set>
#include <thread>
#include <vector>

#include "../../globals.h"
#include "../k8s/K8sWorkerController.h"
#include "../util/Utils.h"

using namespace std;

std::mutex schedulerMutex;
std::map<std::string, int> used_workers;

static Logger scaler_logger;

static std::thread *scale_down_thread = nullptr;
static volatile bool running = false;
static SQLiteDBInterface *sqlite = nullptr;

static void scale_down_thread_fn();

void start_scale_down(SQLiteDBInterface *sqliteInterface) {
    if (jasminegraph_profile != PROFILE_K8S) return;
    if (scale_down_thread) return;
    sqlite = sqliteInterface;
    running = true;
    scaler_logger.info("Starting scale down thread");
    scale_down_thread = new std::thread(scale_down_thread_fn);
}

void stop_scale_down() {
    if (jasminegraph_profile != PROFILE_K8S) return;
    if (!scale_down_thread) return;
    running = false;
    scale_down_thread->join();
    scale_down_thread = nullptr;
}

static void scale_down_thread_fn() {
    while (running) {
        sleep(30);
        if (!running) break;
        scaler_logger.info("Scale down thread is running");
        schedulerMutex.lock();
        vector<string> workers;  // [worker_id]
        const std::vector<vector<pair<string, string>>> &results =
            sqlite->runSelect("SELECT DISTINCT idworker, ip, server_port FROM worker;");

        map<string, int> loads;
        const map<string, string> &cpu_map = Utils::getMetricMap("cpu_usage");

        for (int i = 0; i < results.size(); i++) {
            string ip = results[i][1].second;
            string port = results[i][2].second;
            const auto workerLoadIt = cpu_map.find(ip + ":" + port);
            if (workerLoadIt != cpu_map.end()) {
                double load = stod(workerLoadIt->second.c_str());
                if (load > 0.25) continue;  // worker is running some task. should not remove this node.
            }
            string workerId = results[i][0].second;
            workers.push_back(workerId);
        }

        set<int> removing;
        cout << "Idle workers = [";
        for (auto it = workers.begin(); it != workers.end(); it++) {
            const auto &worker = *it;
            auto it_used = used_workers.find(worker);
            if (it_used != used_workers.end() && it_used->second > 0) continue;
            removing.insert(stoi(worker));
            cout << worker << ", ";
        }
        cout << "]" << endl;

        int spare = 2;
        if (removing.find(0) != removing.end()) {
            removing.erase(0);
            spare--;
        }
        if (removing.find(1) != removing.end()) {
            removing.erase(1);
            spare--;
        }
        while (spare-- > 0) {
            auto it = removing.begin();
            if (it == removing.end()) break;
            removing.erase(it);
        }
        if (!removing.empty()) {
            K8sWorkerController *k8s = K8sWorkerController::getInstance();
            k8s->scaleDown(removing);
        };

        schedulerMutex.unlock();
    }
}
