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

#include "K8sWorkerController.h"

#include <stdlib.h>

#include <stdexcept>
#include <utility>

Logger controller_logger;

std::vector<JasmineGraphServer::worker> K8sWorkerController::workerList = {};
static int TIME_OUT = 180;
static std::vector<int> activeWorkerIds = {};

static inline int getNextWorkerId() {
    if (activeWorkerIds.empty()) {
        return 0;
    } else {
        return *max_element(activeWorkerIds.begin(), activeWorkerIds.end()) + 1;
    }
}

K8sWorkerController::K8sWorkerController(std::string masterIp, int numberOfWorkers, SQLiteDBInterface *metadb) {
    this->masterIp = std::move(masterIp);
    this->numberOfWorkers = numberOfWorkers;
    this->interface = new K8sInterface();
    this->metadb = *metadb;

    try {
        this->maxWorkers = stoi(this->interface->getJasmineGraphConfig("max_worker_count"));
    } catch (std::invalid_argument &e) {
        controller_logger.error("Invalid max_worker_count value. Defaulted to 4");
        this->maxWorkers = 4;
    }


    // Delete all the workers from the database
    metadb->runUpdate("DELETE FROM worker");
    int workersAttached = this->attachExistingWorkers();
    if (numberOfWorkers - workersAttached > 0){
        this->scaleUp(numberOfWorkers-workersAttached);
    }
}

K8sWorkerController::~K8sWorkerController() { delete this->interface; }

static K8sWorkerController *instance = nullptr;

K8sWorkerController *K8sWorkerController::getInstance() {
    if (instance == nullptr) {
        throw std::runtime_error("K8sWorkerController is not instantiated");
    }
    return instance;
}

K8sWorkerController *K8sWorkerController::getInstance(std::string masterIp, int numberOfWorkers,
                                                      SQLiteDBInterface *metadb) {
    // TODO(thevindu-w): synchronize
    if (instance == nullptr) {
        instance = new K8sWorkerController(masterIp, numberOfWorkers, metadb);
    } else {
        controller_logger.warn("Not initializing again");
    }
    return instance;
}

std::string K8sWorkerController::spawnWorker(int workerId) {
    auto volume = this->interface->createJasmineGraphPersistentVolume(workerId);
    if (volume != nullptr && volume->metadata != nullptr && volume->metadata->name != nullptr) {
        controller_logger.info("Worker " + std::to_string(workerId) + " persistent volume created successfully");
    } else {
        throw std::runtime_error("Worker " + std::to_string(workerId) + " persistent volume creation failed");
    }

    auto claim = this->interface->createJasmineGraphPersistentVolumeClaim(workerId);
    if (claim != nullptr && claim->metadata != nullptr && claim->metadata->name != nullptr) {
        controller_logger.info("Worker " + std::to_string(workerId) + " persistent volume claim created successfully");
    } else {
        throw std::runtime_error("Worker " + std::to_string(workerId) + " persistent volume claim creation failed");
    }

    v1_service_t *service = this->interface->createJasmineGraphWorkerService(workerId);
    if (service != nullptr && service->metadata != nullptr && service->metadata->name != nullptr) {
        controller_logger.info("Worker " + std::to_string(workerId) + " service created successfully");
    } else {
        throw std::runtime_error("Worker " + std::to_string(workerId) + " service creation failed");
    }

    std::string ip(service->spec->cluster_ip);

    v1_deployment_t *deployment =
        this->interface->createJasmineGraphWorkerDeployment(workerId, ip, this->masterIp);
    if (deployment != nullptr && deployment->metadata != nullptr && deployment->metadata->name != nullptr) {
        controller_logger.info("Worker " + std::to_string(workerId) + " deployment created successfully");

    } else {
        throw std::runtime_error("Worker " + std::to_string(workerId) + " deployment creation failed");
    }

    int sockfd;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        controller_logger.error("Cannot create socket");
        return "";
    }

    server = gethostbyname(ip.c_str());
    if (server == NULL) {
        controller_logger.error("ERROR, no host named " + ip);
        return "";
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(Conts::JASMINEGRAPH_INSTANCE_PORT);

    int waiting = 0;
    while (true) {
        if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
            waiting += 30; // Added overhead in connection retry attempts
            sleep(10);
        } else {
            Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::CLOSE);
            close(sockfd);
            break;
        }

        if (waiting >= TIME_OUT) {
            controller_logger.error("Error in spawning new worker");
            deleteWorker(workerId);
            return "";
        }

    }

    JasmineGraphServer::worker worker = {
        .hostname = ip, .port = Conts::JASMINEGRAPH_INSTANCE_PORT, .dataPort = Conts::JASMINEGRAPH_INSTANCE_DATA_PORT};
    K8sWorkerController::workerList.push_back(worker);
    std::string insertQuery =
        "INSERT INTO worker (host_idhost, server_port, server_data_port, name, ip, idworker) "
        "VALUES ( -1, " + std::to_string(Conts::JASMINEGRAPH_INSTANCE_PORT) + ", " +
        std::to_string(Conts::JASMINEGRAPH_INSTANCE_DATA_PORT) + ", " + "'" + std::string(service->metadata->name) +
        "', " + "'" + ip + "', " + std::to_string(workerId) + ")";
    int status = metadb.runInsert(insertQuery);
    if (status == -1) {
        controller_logger.error("Worker " + std::to_string(workerId) + " database insertion failed");
    }
    activeWorkerIds.push_back(workerId);
    return ip + ":" + to_string(Conts::JASMINEGRAPH_INSTANCE_PORT);
}

void K8sWorkerController::deleteWorker(int workerId) {
    std::string selectQuery = "SELECT ip, server_port FROM worker WHERE idworker = " + std::to_string(workerId);
    auto result = metadb.runSelect(selectQuery);
    if (result.size() == 0) {
        controller_logger.error("Worker " + std::to_string(workerId) + " not found in the database");
        return;
    }
    std::string ip = result.at(0).at(0).second;
    int port = atoi(result.at(0).at(1).second.c_str());

    int response = JasmineGraphServer::shutdown_worker(ip, port);
    if (response == -1) {
        controller_logger.error("Worker " + std::to_string(workerId) + " graceful shutdown failed");
    }

    v1_status_t *status = this->interface->deleteJasmineGraphWorkerDeployment(workerId);
    if (status != nullptr && status->code == 0) {
        controller_logger.info("Worker " + std::to_string(workerId) + " deployment deleted successfully");
    } else {
        controller_logger.error("Worker " + std::to_string(workerId) + " deployment deletion failed");
    }

    v1_service_t *service = this->interface->deleteJasmineGraphWorkerService(workerId);
    if (service != nullptr && service->metadata != nullptr && service->metadata->name != nullptr) {
        controller_logger.info("Worker " + std::to_string(workerId) + " service deleted successfully");
    } else {
        controller_logger.error("Worker " + std::to_string(workerId) + " service deletion failed");
    }

    auto volume = this->interface->deleteJasmineGraphPersistentVolume(workerId);
    if (volume != nullptr && volume->metadata != nullptr && volume->metadata->name != nullptr) {
        controller_logger.info("Worker " + std::to_string(workerId) + " persistent volume deleted successfully");
    } else {
        controller_logger.error("Worker " + std::to_string(workerId) + " persistent volume deletion failed");
    }

    auto claim = this->interface->deleteJasmineGraphPersistentVolumeClaim(workerId);
    if (claim != nullptr && claim->metadata != nullptr && claim->metadata->name != nullptr) {
        controller_logger.info("Worker " + std::to_string(workerId) + " persistent volume claim deleted successfully");
    } else {
        controller_logger.error("Worker " + std::to_string(workerId) + " persistent volume claim deletion failed");
    }

    std::string deleteQuery = "DELETE FROM worker WHERE idworker = " + std::to_string(workerId);
    metadb.runUpdate(deleteQuery);
    std::remove(activeWorkerIds.begin(), activeWorkerIds.end(), workerId);
}

int K8sWorkerController::attachExistingWorkers() {
    v1_deployment_list_t *deployment_list =
        this->interface->getDeploymentList(strdup("deployment=jasminegraph-worker"));

    if (deployment_list && deployment_list->items->count > 0) {
        listEntry_t *listEntry = NULL;
        v1_deployment_t *deployment = NULL;
        list_ForEach(listEntry, deployment_list->items) {
            deployment = static_cast<v1_deployment_t *>(listEntry->data);
            list_t *labels = deployment->metadata->labels;
            listEntry_t *label = NULL;

            list_ForEach(label, labels) {
                auto *pair = static_cast<keyValuePair_t *>(label->data);
                v1_service_t *service;
                if (strcmp(pair->key, "workerId") == 0) {
                    int workerId = std::stoi(static_cast<char *>(pair->value));
                    v1_service_list_t *service_list = this->interface->getServiceList(
                        strdup(("service=jasminegraph-worker,workerId=" + std::to_string(workerId)).c_str()));

                    if (!service_list || service_list->items->count == 0) {
                        service = this->interface->createJasmineGraphWorkerService(workerId);
                    } else {
                        service = static_cast<v1_service_t *>(service_list->items->firstEntry->data);
                    }

                    std::string ip(service->spec->cluster_ip);
                    JasmineGraphServer::worker worker = {.hostname = ip,
                                                         .port = Conts::JASMINEGRAPH_INSTANCE_PORT,
                                                         .dataPort = Conts::JASMINEGRAPH_INSTANCE_DATA_PORT};
                    K8sWorkerController::workerList.push_back(worker);
                    std::string insertQuery =
                        "INSERT INTO worker (host_idhost, server_port, server_data_port, name, ip, idworker) "
                        "VALUES ( -1, " + std::to_string(Conts::JASMINEGRAPH_INSTANCE_PORT) + ", " +
                        std::to_string(Conts::JASMINEGRAPH_INSTANCE_DATA_PORT) + ", " + "'" +
                        std::string(service->metadata->name) + "', " + "'" + ip + "', " + std::to_string(workerId) +
                        ")";
                    int status = metadb.runInsert(insertQuery);
                    if (status == -1) {
                        controller_logger.error("Worker " + std::to_string(workerId) + " database insertion failed");
                    }
                    activeWorkerIds.push_back(workerId);
                    break;
                }
            }
        }
        return deployment_list->items->count;
    } else {
        return 0;
    }
}

std::string K8sWorkerController::getMasterIp() const { return this->masterIp; }

int K8sWorkerController::getNumberOfWorkers() const { return numberOfWorkers; }

/*
 * @deprecated
 */
void K8sWorkerController::setNumberOfWorkers(int newNumberOfWorkers) {
    if (newNumberOfWorkers > this->numberOfWorkers) {
        for (int i = this->numberOfWorkers; i < newNumberOfWorkers; i++) {
            this->spawnWorker(i);
        }
    } else if (newNumberOfWorkers < this->numberOfWorkers) {
        for (int i = newNumberOfWorkers; i < this->numberOfWorkers; i++) {
            this->deleteWorker(i);
        }
    }
    this->numberOfWorkers = newNumberOfWorkers;
}

std::map<string, string> K8sWorkerController::scaleUp(int count) {
    if (this->numberOfWorkers + count > this->maxWorkers) {
        count = this->maxWorkers - this->numberOfWorkers;
    }
    auto *asyncCalls = new std::future<string>[count];
    int nextWorkerId = getNextWorkerId();
    for (int i = 0; i < count; i++) {
        asyncCalls[i] = std::async(&K8sWorkerController::spawnWorker, this, nextWorkerId + i);
    }

    std::map<string, string> workers;
    int success = 0;
    for (int i = 0; i < count; i++) {
        std::string result = asyncCalls[i].get();
        if (!result.empty()) {
            success++;
            workers.insert(std::make_pair(to_string(i), result));
        }
    }
    this->numberOfWorkers += success;
    return workers;
}
