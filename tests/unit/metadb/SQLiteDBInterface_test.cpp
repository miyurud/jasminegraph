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

#include "../../../src/metadb/SQLiteDBInterface.h"

#include <string>
#include "gtest/gtest.h"

class SQLiteDBInterfaceTest : public ::testing::Test {
 protected:
  SQLiteDBInterface *dbInterface = NULL;

  void SetUp() override {
    dbInterface = new SQLiteDBInterface(TEST_RESOURCE_DIR "temp/jasminegraph_meta.db");
    dbInterface->init();
  }

  void TearDown() override {
    dbInterface->finalize();
    delete dbInterface;
    remove(TEST_RESOURCE_DIR "temp/jasminegraph_meta.db");
  }
};

TEST_F(SQLiteDBInterfaceTest, TestRunSelect) {
    std::vector<std::vector<std::pair<std::basic_string<char>, std::basic_string<char>>>> result;
    std::string tables[] =
        {"graph", "host", "model", "partition", "worker", "worker_has_partition"};
    for (const auto& table : tables) {
        result = dbInterface->runSelect("SELECT * FROM " + table);
        ASSERT_EQ(result.size(), 0);
    }

    result = dbInterface->runSelect("SELECT * FROM graph_status");
    ASSERT_EQ(result.size(), 4);
}

TEST_F(SQLiteDBInterfaceTest, TestRunInsertAndUpdate) {
    auto result =
        dbInterface->runInsert(
            "INSERT INTO host('name', 'ip', 'is_public') VALUES('localhost', '127.0.0.1', 'false')");
    ASSERT_EQ(result, 1);

    auto data = dbInterface->runSelect("SELECT * FROM host");
    ASSERT_EQ(data.size(), 1);
    ASSERT_EQ(data[0][0].second, "1");
    ASSERT_EQ(data[0][1].second, "localhost");
    ASSERT_EQ(data[0][2].second, "127.0.0.1");
    ASSERT_EQ(data[0][3].second, "false");

    dbInterface->runUpdate("UPDATE host SET name='localhost2' WHERE idhost=1");
    data = dbInterface->runSelect("SELECT * FROM host");
    ASSERT_EQ(data.size(), 1);
    ASSERT_EQ(data[0][0].second, "1");
    ASSERT_EQ(data[0][1].second, "localhost2");
    ASSERT_EQ(data[0][2].second, "127.0.0.1");
    ASSERT_EQ(data[0][3].second, "false");
}
