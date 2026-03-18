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

#include "../../../src/k8s/K8sWorkerController.h"

#include <fstream>
#include <iostream>
#include <memory>
#include <vector>

#include "gtest/gtest.h"

class K8sWorkerControllerTest : public ::testing::Test {
 public:
    static K8sWorkerController *controller;
    static SQLiteDBInterface *metadb;
    static K8sInterface *interface;

    static void cleanupAllWorkerResources() {
        auto cleanupInterface = std::make_unique<K8sInterface>();
        // Delete all jasminegraph-worker deployments, services, PVCs, and PVs
        v1_deployment_list_t *deployments =
            cleanupInterface->getDeploymentList(strdup("deployment=jasminegraph-worker"));
        if (deployments && deployments->items && deployments->items->count > 0) {
            listEntry_t *entry = NULL;
            list_ForEach(entry, deployments->items) {
                auto *dep = static_cast<v1_deployment_t *>(entry->data);
                if (dep && dep->metadata && dep->metadata->labels) {
                    listEntry_t *label = NULL;
                    list_ForEach(label, dep->metadata->labels) {
                        auto *pair = static_cast<keyValuePair_t *>(label->data);
                        if (strcmp(pair->key, "workerId") == 0) {
                            int wId = std::stoi(static_cast<char *>(pair->value));
                            try { 
                                cleanupInterface->deleteJasmineGraphWorkerDeployment(wId); 
                            } catch (const std::runtime_error& e) { 
                                std::cerr << "Cleanup: " << e.what() << std::endl; 
                            }
                            try { 
                                cleanupInterface->deleteJasmineGraphWorkerService(wId); 
                            } catch (const std::runtime_error& e) { 
                                std::cerr << "Cleanup: " << e.what() << std::endl; 
                            }
                            try { 
                                cleanupInterface->deleteJasmineGraphPersistentVolumeClaim(wId); 
                            } catch (const std::runtime_error& e) { 
                                std::cerr << "Cleanup: " << e.what() << std::endl; 
                            }
                            try { 
                                cleanupInterface->deleteJasmineGraphPersistentVolume(wId); 
                            } catch (const std::runtime_error& e) { 
                                std::cerr << "Cleanup: " << e.what() << std::endl; 
                            }
                            break;
                        }
                    }
                }
            }
        }
        // Also clean up services that might exist without deployments
        v1_service_list_t *services =
            cleanupInterface->getServiceList(strdup("service=jasminegraph-worker"));
        if (services && services->items && services->items->count > 0) {
            listEntry_t *entry = NULL;
            list_ForEach(entry, services->items) {
                auto *svc = static_cast<v1_service_t *>(entry->data);
                if (svc && svc->metadata && svc->metadata->labels) {
                    listEntry_t *label = NULL;
                    list_ForEach(label, svc->metadata->labels) {
                        auto *pair = static_cast<keyValuePair_t *>(label->data);
                        if (strcmp(pair->key, "workerId") == 0) {
                            int wId = std::stoi(static_cast<char *>(pair->value));
                            try { cleanupInterface->deleteJasmineGraphWorkerService(wId); } catch (const std::runtime_error& e) { std::cerr << "Cleanup: " << e.what() << std::endl; }
                            break;
                        }
                    }
                }
            }
        }

    }

    static void SetUpTestSuite() {
        // Clean up any pre-existing jasminegraph-worker resources in the cluster
        cleanupAllWorkerResources();

        metadb = new SQLiteDBInterface(TEST_RESOURCE_DIR "temp/jasminegraph_meta.db");
        metadb->init();
        controller = K8sWorkerController::getInstance("10.43.0.1", 2, metadb);
        interface = new K8sInterface();
    }

    static void TearDownTestSuite() {
        // Clean up all workers created during tests
        cleanupAllWorkerResources();

        delete controller;
        delete metadb;
        delete interface;
        remove(TEST_RESOURCE_DIR "temp/jasminegraph_meta.db");
    }
};

K8sWorkerController *K8sWorkerControllerTest::controller = nullptr;
SQLiteDBInterface *K8sWorkerControllerTest::metadb = nullptr;
K8sInterface *K8sWorkerControllerTest::interface = nullptr;

TEST_F(K8sWorkerControllerTest, TestConstructor) {
    ASSERT_EQ(controller->getMasterIp(), "10.43.0.1");
    ASSERT_EQ(controller->getNumberOfWorkers(), 2);
    auto result = metadb->runSelect("SELECT idworker FROM worker");
    ASSERT_EQ(result.size(), 2);

    v1_deployment_list_t *deployment_list = interface->getDeploymentList(strdup("deployment=jasminegraph-worker"));
    ASSERT_EQ(deployment_list->items->count, 2);
    v1_service_list_t *service_list = interface->getServiceList(strdup("service=jasminegraph-worker"));
    ASSERT_EQ(service_list->items->count, 2);
}

TEST_F(K8sWorkerControllerTest, TestScaleUp) {
    auto result = controller->scaleUp(2);
    ASSERT_EQ(result.size(), 2);
    v1_deployment_list_t *deployment_list = interface->getDeploymentList(strdup("deployment=jasminegraph-worker"));
    ASSERT_EQ(deployment_list->items->count, 4);
    v1_service_list_t *service_list = interface->getServiceList(strdup("service=jasminegraph-worker"));
    ASSERT_EQ(service_list->items->count, 4);
}

TEST_F(K8sWorkerControllerTest, TestScaleUpBeyondLimit) {
    auto result = controller->scaleUp(1);
    ASSERT_EQ(result.size(), 0);
    v1_deployment_list_t *deployment_list = interface->getDeploymentList(strdup("deployment=jasminegraph-worker"));
    ASSERT_EQ(deployment_list->items->count, 4);
    v1_service_list_t *service_list = interface->getServiceList(strdup("service=jasminegraph-worker"));
    ASSERT_EQ(service_list->items->count, 4);
}
