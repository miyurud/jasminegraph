
#include "gtest/gtest.h"

#include "../../../src/util/Utils.h"

std::string sample = "apiVersion: v1\n"
                     "kind: Pod\n"
                     "metadata:\n"
                     "  labels:\n"
                     "    service: <name>\n"
                     "spec:\n"
                     "  containers:\n"
                     "    - name: <name>";

TEST(UtilsTest, TestGetJasmineGraphProperty) {
    ASSERT_EQ(Utils::getJasmineGraphProperty("org.jasminegraph.server.host"), "localhost");
}

TEST(UtilsTest, TestGetFileContentAsString) {
    auto actual = Utils::getFileContentAsString(TEST_RESOURCE_DIR"sample.yaml");
    ASSERT_EQ(sample, actual);
}

TEST(UtilsTest, TestReplaceAll) {
    std::string expected = "apiVersion: v1\n"
                           "kind: Pod\n"
                           "metadata:\n"
                           "  labels:\n"
                           "    service: jasminegraph\n"
                           "spec:\n"
                           "  containers:\n"
                           "    - name: jasminegraph";
    std::string actual = Utils::replaceAll(sample, "<name>", "jasminegraph");
    ASSERT_EQ(actual, expected);
}

TEST(UtilsTest, TestWriteFileContent) {
    Utils::writeFileContent(TEST_RESOURCE_DIR"temp/sample.yaml", sample);
    std::string actual = Utils::getFileContentAsString(TEST_RESOURCE_DIR"temp/sample.yaml");
    ASSERT_EQ(actual, sample);
}

TEST(UtilsTest, TestGetJsonStringFromYamlFile) {
    std::string expected = R"({"apiVersion":"v1","kind":"Pod","metadata":{"labels":{"service":"<name>"}},"spec":{"containers":[{"name":"<name>"}]}})";
    std::string actual = Utils::getJsonStringFromYamlFile(TEST_RESOURCE_DIR"sample.yaml");
    ASSERT_EQ(actual, expected);
}