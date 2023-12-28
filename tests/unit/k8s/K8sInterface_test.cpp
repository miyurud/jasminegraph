
#include "gtest/gtest.h"

#include "../../../src/k8s/K8sInterface.h"

class K8sInterfaceTest : public ::testing::Test {
protected:
    K8sInterface* interface{};
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

TEST_F(K8sInterfaceTest, TestGetPodList) {
    v1_pod_list_t *pod_list = interface->getPodList("app=gtest-nginx");
    ASSERT_TRUE(pod_list->items->count == 0);
}

TEST_F(K8sInterfaceTest, TestGetServiceList) {
    v1_service_list_t *service_list = interface->getServiceList("app=gtest-nginx");
    ASSERT_TRUE(service_list->items->count == 0);
}
