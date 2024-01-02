
#include "gtest/gtest.h"

#include "../../../src/k8s/K8sInterface.h"

class K8sInterfaceTest : public ::testing::Test {
 protected:
    K8sInterface *interface{};

    void SetUp() override {
        interface = new K8sInterface();
    }

    void TearDown() override {
        delete interface;
    }
};

TEST_F(K8sInterfaceTest, TestConstructor) {
    ASSERT_NE(interface->apiClient, nullptr);
}

TEST_F(K8sInterfaceTest, TestGetDeploymentList) {
    v1_deployment_list_t *deployment_list = interface->getDeploymentList(strdup("app=gtest-nginx"));
    ASSERT_EQ(deployment_list->items->count, 0);
}

TEST_F(K8sInterfaceTest, TestGetServiceList) {
    v1_service_list_t *service_list = interface->getServiceList(strdup("app=gtest-nginx"));
    ASSERT_EQ(service_list->items->count, 0);
}

TEST_F(K8sInterfaceTest, TestCreateJasmineGraphWorkerDeployment) {
    v1_deployment_t *deployment = interface->createJasmineGraphWorkerDeployment(1, "10.43.0.1");
    ASSERT_STREQ(deployment->metadata->name, "jasminegraph-worker1-deployment");
    ASSERT_EQ(interface->apiClient->response_code, 201);
}

TEST_F(K8sInterfaceTest, TestCreateJasmineGraphWorkerService) {
    v1_service_t *service = interface->createJasmineGraphWorkerService(1);
    ASSERT_STREQ(service->metadata->name, "jasminegraph-worker1-service");
    ASSERT_NE(service->spec->cluster_ip, nullptr);
    ASSERT_EQ(interface->apiClient->response_code, 201);
}

TEST_F(K8sInterfaceTest, TestGetDeploymentListAfterDeployment) {
    v1_deployment_list_t *deployment_list = interface->getDeploymentList(strdup("deployment=jasminegraph-worker"));
    ASSERT_EQ(deployment_list->items->count, 1);
}

TEST_F(K8sInterfaceTest, TestGetServiceListAfterServiceCreation) {
    v1_service_list_t *service_list = interface->getServiceList(strdup("service=jasminegraph-worker"));
    ASSERT_EQ(service_list->items->count, 1);
}

TEST_F(K8sInterfaceTest, TestDeleteJasmineGraphWorkerDeployment) {
    v1_status_t *status = interface->deleteJasmineGraphWorkerDeployment(1);
    ASSERT_EQ(status->code, 0);
    ASSERT_EQ(interface->apiClient->response_code, 200);
}

TEST_F(K8sInterfaceTest, TestDeleteJasmineGraphWorkerService) {
    v1_service_t *service = interface->deleteJasmineGraphWorkerService(1);
    ASSERT_STREQ(service->metadata->name, "jasminegraph-worker1-service");
    ASSERT_EQ(interface->apiClient->response_code, 200);
}
