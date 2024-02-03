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

#include "../util/Conts.h"
#include "../util/logger/Logger.h"
#include "../server/JasmineGraphServer.h"

Logger controller_logger;

K8sWorkerController::K8sWorkerController(std::string masterIp, int numberOfWorkers, SQLiteDBInterface *metadb) {
    this->masterIp = std::move(masterIp);
    this->numberOfWorkers = numberOfWorkers;
    this->interface = new K8sInterface();
    this->metadb = *metadb;

    // Delete all hosts
    metadb->runUpdate("DELETE FROM host");

    v1_node_list_t *nodeList = interface->getNodes();
    listEntry_t *listEntry = NULL;
    v1_node_t *node = NULL;

    std::string query;
    list_ForEach(listEntry, nodeList->items) {
        node = static_cast<v1_node_t *>(listEntry->data);
        query = "INSERT INTO host (name, ip) VALUES ('" + std::string(node->metadata->name) + "', '')";
        metadb->runInsert(query);
    }

    query = "SELECT idhost, name FROM host";
    for (auto entry : metadb->runSelect(query)) {
        nodes[entry.at(1).second] = atoi(entry.at(0).second.c_str());
    }

    // Delete all the workers from the database
    metadb->runUpdate("DELETE FROM worker");
    int workersAttached = this->attachExistingWorkers();
    for (int i = workersAttached; i < numberOfWorkers; i++) {
        this->spawnWorker(i);
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

void K8sWorkerController::spawnWorker(int workerId) {
    // TODO (M-I-M-Ishad): Develop criteria to assign the worker to node based on performance metrics
    // getting a node randomly
    auto it = nodes.begin();
    std::advance(it, rand() % nodes.size());
    std::string hostName = it->first;
    int hostId = it->second;

    v1_deployment_t *deployment =
        this->interface->createJasmineGraphWorkerDeployment(workerId, this->masterIp, hostName);
    if (deployment != nullptr && deployment->metadata != nullptr && deployment->metadata->name != nullptr) {
        controller_logger.info("Worker " + std::to_string(workerId) + " deployment created successfully");

    } else {
        throw std::runtime_error("Worker " + std::to_string(workerId) + " deployment creation failed");
    }

    v1_service_t *service = this->interface->createJasmineGraphWorkerService(workerId);
    if (service != nullptr && service->metadata != nullptr && service->metadata->name != nullptr) {
        controller_logger.info("Worker " + std::to_string(workerId) + " service created successfully");
    } else {
        throw std::runtime_error("Worker " + std::to_string(workerId) + " service creation failed");
    }

    std::string insertQuery =
        "INSERT INTO worker (host_idhost, server_port, server_data_port, name, ip, idworker) "
        "VALUES (" +
        std::to_string(hostId) + ", " + std::to_string(Conts::JASMINEGRAPH_INSTANCE_PORT) + ", " +
        std::to_string(Conts::JASMINEGRAPH_INSTANCE_DATA_PORT) + ", " + "'" + std::string(service->metadata->name) +
        "', " + "'" + std::string(service->spec->cluster_ip) + "', " + std::to_string(workerId) + ")";
    int status = metadb.runInsert(insertQuery);
    if (status == -1) {
        controller_logger.error("Worker " + std::to_string(workerId) + " database insertion failed");
    }
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

    std::string deleteQuery = "DELETE FROM worker WHERE idworker = " + std::to_string(workerId);
    metadb.runUpdate(deleteQuery);
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
            std::string nodeName = deployment->spec->_template->spec->node_name;
            int hostId = nodes[nodeName];
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

                    std::string insertQuery =
                        "INSERT INTO worker (host_idhost, server_port, server_data_port, name, ip, idworker) "
                        "VALUES ( " +
                        std::to_string(hostId) + ", " + std::to_string(Conts::JASMINEGRAPH_INSTANCE_PORT) + ", " +
                        std::to_string(Conts::JASMINEGRAPH_INSTANCE_DATA_PORT) + ", " + "'" +
                        std::string(service->metadata->name) + "', " + "'" + std::string(service->spec->cluster_ip) +
                        "', " + std::to_string(workerId) + ")";
                    int status = metadb.runInsert(insertQuery);
                    if (status == -1) {
                        controller_logger.error("Worker " + std::to_string(workerId) + " database insertion failed");
                    }
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
