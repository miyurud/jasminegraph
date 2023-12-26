
#include "gtest/gtest.h"

#include "../../../src/util/Utils.h"

TEST(UtilsTest, TestGetJasmineGraphProperty) {
    ASSERT_EQ(Utils::getJasmineGraphProperty("org.jasminegraph.server.host"), "localhost");
}
