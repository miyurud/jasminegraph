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

#include "../../../src/performancedb/PerformanceSQLiteDBInterface.h"

#include <string>
#include "gtest/gtest.h"

class PerformanceSQLiteDBInterfaceTest : public ::testing::Test {
 protected:
    PerformanceSQLiteDBInterface *perfdbInterface = NULL;

    void SetUp() override {
        perfdbInterface = new PerformanceSQLiteDBInterface(TEST_RESOURCE_DIR "temp/jasminegraph_performance.db");
        perfdbInterface->init();
    }

    void TearDown() override {
        perfdbInterface->finalize();
        delete perfdbInterface;
        remove(TEST_RESOURCE_DIR "temp/jasminegraph_performance.db");
    }
};

TEST_F(PerformanceSQLiteDBInterfaceTest, TestRunSelect) {
    std::string tables[] =
        {"graph_performance_data",
         "graph_place_sla_performance",
         "graph_sla",
         "host",
         "host_performance_data",
         "place",
         "place_performance_data"};
    for (const auto &table : tables) {
        auto result = perfdbInterface->runSelect("SELECT * FROM " + table);
        ASSERT_EQ(result.size(), 0);
    }

    auto result = perfdbInterface->runSelect("SELECT * FROM sla_category");
    ASSERT_EQ(result.size(), 2);
}

TEST_F(PerformanceSQLiteDBInterfaceTest, TestRunInsertAndUpdate) {
    auto result =
        perfdbInterface->runInsert(
            "INSERT INTO host('name', 'ip', 'is_public', 'total_cpu_cores', 'total_memory') "
            "VALUES('localhost', '127.0.0.1', 'false', 4, '16')");
    ASSERT_EQ(result, 1);

    auto data = perfdbInterface->runSelect("SELECT * FROM host");
    ASSERT_EQ(data.size(), 1);
    ASSERT_EQ(data[0][0].second, "1");
    ASSERT_EQ(data[0][1].second, "localhost");
    ASSERT_EQ(data[0][2].second, "127.0.0.1");
    ASSERT_EQ(data[0][3].second, "false");
    ASSERT_EQ(data[0][4].second, "4");
    ASSERT_EQ(data[0][5].second, "16");

    perfdbInterface->runUpdate("UPDATE host SET name='localhost2' WHERE idhost=1");
    data = perfdbInterface->runSelect("SELECT * FROM host");
    ASSERT_EQ(data.size(), 1);
    ASSERT_EQ(data[0][0].second, "1");
    ASSERT_EQ(data[0][1].second, "localhost2");
    ASSERT_EQ(data[0][2].second, "127.0.0.1");
    ASSERT_EQ(data[0][3].second, "false");
    ASSERT_EQ(data[0][4].second, "4");
    ASSERT_EQ(data[0][5].second, "16");
}
