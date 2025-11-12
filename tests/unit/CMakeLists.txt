cmake_minimum_required(VERSION 3.29.6)
project(JasmineGraphUnitTest)
find_package(OpenMP REQUIRED)
set(SOURCES
        main.cpp
        util/Utils_test.cpp
        k8s/K8sInterface_test.cpp
        k8s/K8sWorkerController_test.cpp
        metadb/SQLiteDBInterface_test.cpp
        performancedb/PerformanceSQLiteDBInterface_test.cpp)

add_executable(${PROJECT_NAME} ${SOURCES})
target_link_libraries(${PROJECT_NAME} OpenMP::OpenMP_CXX gtest gtest_main JasmineGraphLib)
target_compile_definitions(${PROJECT_NAME} PUBLIC TEST_RESOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/resources/")

add_test(NAME ${PROJECT_NAME} COMMAND ${PROJECT_NAME})


