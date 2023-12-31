
#include "gtest/gtest.h"

#include "../../../src/k8s/K8sInterface.h"

TEST(K8sInterfaceTest, TestDeployObjectFromFile) {
    int result = K8sInterface::deployFromDefinitionFile(TEST_RESOURCE_DIR"sample_pod.yaml");
    ASSERT_EQ(result, 0);
}
