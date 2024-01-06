/**
Copyright 2020-2024 JasmineGraph Team
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

#include "../../../src/util/Utils.h"

#include <vector>

#include "gtest/gtest.h"

TEST(UtilsTest, TestGetJasmineGraphProperty) {
    ASSERT_EQ(Utils::getJasmineGraphProperty("org.jasminegraph.server.host"), "localhost");
}

TEST(UtilsTest, TestGetListOfFilesInDirectory) {
    std::vector<std::string> flist = Utils::getListOfFilesInDirectory(".");
    ASSERT_EQ(flist.size(), 6);

    std::sort(flist.begin(), flist.end());
    ASSERT_EQ(flist.at(0), "CMakeCache.txt");
    ASSERT_EQ(flist.at(1), "CTestTestfile.cmake");
    ASSERT_EQ(flist.at(2), "JasmineGraph");
    ASSERT_EQ(flist.at(3), "Makefile");
    ASSERT_EQ(flist.at(4), "cmake_install.cmake");
    ASSERT_EQ(flist.at(5), "libJasmineGraphLib.a");
}
