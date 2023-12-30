
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
