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
#include <vector>

#include "gtest/gtest.h"

class K8sWorkerControllerTest : public ::testing::Test {
 protected:
    K8sWorkerController *controller{};
    SQLiteDBInterface *metadb = NULL;
    K8sInterface *interface {};

    void SetUp() override {
        std::ifstream src(ROOT_DIR "/metadb/jasminegraph_meta.db", std::ios::binary);
        std::ofstream dst(TEST_RESOURCE_DIR "/temp/jasminegraph_meta.db", std::ios::binary);
        dst << src.rdbuf();

        metadb = new SQLiteDBInterface(TEST_RESOURCE_DIR "/temp/jasminegraph_meta.db");
        metadb->init();
        controller = new K8sWorkerController("10.43.0.1", 2, metadb);
        interface = new K8sInterface();
    }

    void TearDown() override {
        delete controller;
        delete metadb;
        delete interface;
        remove(TEST_RESOURCE_DIR "/temp/jasminegraph_meta.db");
    }
};

TEST_F(K8sWorkerControllerTest, TestConstructor) {
    ASSERT_EQ(controller->getMasterIp(), "10.43.0.1");
    ASSERT_EQ(controller->getNumberOfWorkers(), 2);
    auto result = metadb->runSelect("SELECT * FROM worker");
    ASSERT_EQ(result.size(), 2);

    v1_deployment_list_t *deployment_list = interface->getDeploymentList(strdup("deployment=jasminegraph-worker"));
    ASSERT_EQ(deployment_list->items->count, 2);
    v1_service_list_t *service_list = interface->getServiceList(strdup("service=jasminegraph-worker"));
    ASSERT_EQ(service_list->items->count, 2);
}

TEST_F(K8sWorkerControllerTest, TestScalingUpAndDown) {
    controller->setNumberOfWorkers(4);
    ASSERT_EQ(controller->getNumberOfWorkers(), 4);
    auto result = metadb->runSelect("SELECT * FROM worker");
    ASSERT_EQ(result.size(), 4);

    v1_deployment_list_t *deployment_list = interface->getDeploymentList(strdup("deployment=jasminegraph-worker"));
    ASSERT_EQ(deployment_list->items->count, 4);
    v1_service_list_t *service_list = interface->getServiceList(strdup("service=jasminegraph-worker"));
    ASSERT_EQ(service_list->items->count, 4);

    controller->setNumberOfWorkers(0);
    ASSERT_EQ(controller->getNumberOfWorkers(), 0);
    result = metadb->runSelect("SELECT * FROM worker");
    ASSERT_EQ(result.size(), 0);

    deployment_list = interface->getDeploymentList(strdup("deployment=jasminegraph-worker"));
    ASSERT_EQ(deployment_list->items->count, 0);
    service_list = interface->getServiceList(strdup("service=jasminegraph-worker"));
    ASSERT_EQ(service_list->items->count, 0);
}
