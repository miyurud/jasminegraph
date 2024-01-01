
#include <fstream>
#include <vector>
#include "gtest/gtest.h"
#include "../../../src/k8s/K8sWorkerController.h"

class K8sWorkerControllerTest : public ::testing::Test {
protected:
    K8sWorkerController *controller{};
    SQLiteDBInterface metadb;

    void SetUp() override {
        std::ifstream  src(ROOT_DIR "/metadb/jasminegraph_meta.db", std::ios::binary);
        std::ofstream  dst(TEST_RESOURCE_DIR "/temp/jasminegraph_meta.db",   std::ios::binary);
        dst << src.rdbuf();

        metadb = SQLiteDBInterface(TEST_RESOURCE_DIR "/temp/jasminegraph_meta.db");
        controller = new K8sWorkerController("10.43.0.1", 4, &metadb);
    }

    void TearDown() override {
        delete controller;
        remove(TEST_RESOURCE_DIR "/temp/jasminegraph_meta.db");
    }
};

TEST_F(K8sWorkerControllerTest, TestConstructor) {
    ASSERT_EQ(controller->masterIp, "10.43.0.1");
    ASSERT_EQ(controller->numberOfWorkers, 4);
    auto result = metadb.runSelect("SELECT * FROM worker");
    ASSERT_EQ(result.size(), 4);
}
