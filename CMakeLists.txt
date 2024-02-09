cmake_minimum_required(VERSION 3.10)
project(JasmineGraph)
set(CMAKE_CXX_STANDARD 11)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

set(HEADERS src/backend/JasmineGraphBackend.h
        src/backend/JasmineGraphBackendProtocol.h
        src/centralstore/JasmineGraphHashMapCentralStore.h
        src/centralstore/JasmineGraphHashMapDuplicateCentralStore.h
        src/exception/JasmineGraphException.h
        src/frontend/JasmineGraphFrontEnd.h
        src/frontend/JasmineGraphFrontEndProtocol.h
        src/frontend/core/CoreConstants.h
        src/frontend/core/domain/JobRequest.h
        src/frontend/core/domain/JobResponse.h
        src/frontend/core/executor/AbstractExecutor.h
        src/frontend/core/executor/impl/TriangleCountExecutor.h
        src/frontend/core/executor/impl/StreamingTriangleCountExecutor.h
        src/frontend/core/factory/ExecutorFactory.h
        src/frontend/core/scheduler/JobScheduler.h
        src/localstore/JasmineGraphHashMapLocalStore.h
        src/localstore/JasmineGraphLocalStore.h
        src/localstore/JasmineGraphLocalStoreFactory.h
        src/localstore/incremental/JasmineGraphIncrementalLocalStore.h
        src/metadb/SQLiteDBInterface.h
        src/ml/trainer/JasmineGraphTrainingSchedular.h
        src/partitioner/local/JSONParser.h
        src/partitioner/local/MetisPartitioner.h
        src/partitioner/local/RDFParser.h
        src/partitioner/local/RDFPartitioner.h
        src/partitioner/stream/JasmineGraphIncrementalStore.h
        src/partitioner/stream/Partition.h
        src/partitioner/stream/Partitioner.h
        src/performance/metrics/PerformanceUtil.h
        src/performance/metrics/StatisticCollector.h
        src/performancedb/PerformanceSQLiteDBInterface.h
        src/query/algorithms/linkprediction/JasminGraphLinkPredictor.h
        src/query/algorithms/triangles/Triangles.h
        src/query/algorithms/triangles/StreamingTriangles.h
        src/server/JasmineGraphInstance.h
        src/server/JasmineGraphInstanceFileTransferService.h
        src/server/JasmineGraphInstanceProtocol.h
        src/server/JasmineGraphInstanceService.h
        src/server/JasmineGraphServer.h
        src/util/Conts.h
        src/util/PlacesToNodeMapper.h
        src/util/Utils.h
        src/util/dbutil/attributestore_generated.h
        src/util/dbutil/edgestore_generated.h
        src/util/dbutil/partedgemapstore_generated.h
        src/util/kafka/KafkaCC.h
        src/util/kafka/StreamHandler.h
        src/util/kafka/InstanceStreamHandler.h
        src/util/logger/Logger.h
        src/util/scheduler/Cron.h
        src/util/scheduler/InterruptableSleep.h
        src/util/scheduler/Scheduler.h
        src/util/scheduler/SchedulerService.h
        src/util/scheduler/ctpl_stl.h
        src/k8s/K8sInterface.h
        src/nativestore/NodeManager.h
        src/nativestore/NodeBlock.h
        src/nativestore/PropertyLink.h
        src/nativestore/PropertyEdgeLink.h
        src/nativestore/RelationBlock.h
        src/nativestore/DataPublisher.h
        src/partitioner/stream/Partition.h
        src/k8s/K8sWorkerController.h
        src/streamingdb/StreamingSQLiteDBInterface.h
        src/frontend/core/executor/impl/PageRankExecutor.h
        src/util/dbinterface/DBInterface.h
)

set(SOURCES src/backend/JasmineGraphBackend.cpp
        src/backend/JasmineGraphBackendProtocol.cpp
        src/centralstore/JasmineGraphHashMapCentralStore.cpp
        src/centralstore/JasmineGraphHashMapDuplicateCentralStore.cpp
        src/exception/JasmineGraphException.cpp
        src/frontend/JasmineGraphFrontEnd.cpp
        src/frontend/JasmineGraphFrontEndProtocol.cpp
        src/frontend/core/CoreConstants.cpp
        src/frontend/core/domain/JobRequest.cpp
        src/frontend/core/domain/JobResponse.cpp
        src/frontend/core/executor/AbstractExecutor.cpp
        src/frontend/core/executor/impl/TriangleCountExecutor.cpp
        src/frontend/core/executor/impl/StreamingTriangleCountExecutor.cpp
        src/frontend/core/factory/ExecutorFactory.cpp
        src/frontend/core/scheduler/JobScheduler.cpp
        src/localstore/JasmineGraphHashMapLocalStore.cpp
        src/localstore/JasmineGraphLocalStore.cpp
        src/localstore/JasmineGraphLocalStoreFactory.cpp
        src/localstore/incremental/JasmineGraphIncrementalLocalStore.cpp
        src/metadb/SQLiteDBInterface.cpp
        src/ml/trainer/JasmineGraphTrainingSchedular.cpp
        src/partitioner/local/JSONParser.cpp
        src/partitioner/local/MetisPartitioner.cpp
        src/partitioner/local/RDFParser.cpp
        src/partitioner/local/RDFPartitioner.cpp
        src/partitioner/stream/JasmineGraphIncrementalStore.cpp
        src/partitioner/stream/Partition.cpp
        src/partitioner/stream/Partitioner.cpp
        src/performance/metrics/PerformanceUtil.cpp
        src/performance/metrics/StatisticCollector.cpp
        src/performancedb/PerformanceSQLiteDBInterface.cpp
        src/query/algorithms/linkprediction/JasminGraphLinkPredictor.cpp
        src/query/algorithms/triangles/Triangles.cpp
        src/query/algorithms/triangles/StreamingTriangles.cpp
        src/server/JasmineGraphInstance.cpp
        src/server/JasmineGraphInstanceFileTransferService.cpp
        src/server/JasmineGraphInstanceProtocol.cpp
        src/server/JasmineGraphInstanceService.cpp
        src/server/JasmineGraphServer.cpp
        src/util/Conts.cpp
        src/util/PlacesToNodeMapper.cpp
        src/util/Utils.cpp
        src/util/kafka/KafkaCC.cpp
        src/util/kafka/StreamHandler.cpp
        src/util/kafka/InstanceStreamHandler.cpp
        src/util/logger/Logger.cpp
        src/util/scheduler/SchedulerService.cpp
        src/k8s/K8sInterface.cpp
        src/nativestore/NodeManager.cpp
        src/nativestore/NodeBlock.cpp
        src/nativestore/PropertyLink.cpp
        src/nativestore/PropertyEdgeLink.cpp
        src/nativestore/RelationBlock.cpp
        src/nativestore/DataPublisher.cpp
        src/partitioner/stream/Partition.cpp
        src/k8s/K8sWorkerController.cpp
        src/streamingdb/StreamingSQLiteDBInterface.cpp
        src/frontend/core/executor/impl/PageRankExecutor.cpp
        src/util/dbinterface/DBInterface.cpp
)

add_library(JasmineGraphLib ${HEADERS} ${SOURCES})
add_executable(JasmineGraph main.h main.cpp)

target_compile_definitions(JasmineGraphLib PUBLIC ROOT_DIR="${CMAKE_CURRENT_SOURCE_DIR}/")

if (CMAKE_ENABLE_DEBUG)
    message(STATUS "DEBUG enabled")
    target_compile_options(JasmineGraph PRIVATE -g)
endif ()

#TODO: Need to install the library separately
target_link_libraries(JasmineGraphLib PRIVATE sqlite3)
target_link_libraries(JasmineGraphLib PRIVATE pthread)
target_link_libraries(JasmineGraphLib PRIVATE rdkafka)
target_link_libraries(JasmineGraphLib PRIVATE Threads::Threads)
target_link_libraries(JasmineGraphLib PRIVATE fmt)
target_link_libraries(JasmineGraphLib PRIVATE /usr/lib/x86_64-linux-gnu/libxerces-c.so)
target_link_libraries(JasmineGraphLib PRIVATE /usr/lib/x86_64-linux-gnu/libflatbuffers.a)
target_link_libraries(JasmineGraphLib PRIVATE /usr/lib/x86_64-linux-gnu/libjsoncpp.so)
target_link_libraries(JasmineGraphLib PRIVATE /usr/local/lib/libcppkafka.so)
target_link_libraries(JasmineGraph JasmineGraphLib)
target_include_directories(JasmineGraph PRIVATE /usr/include/python3)
target_link_libraries(JasmineGraph curl)

set(PYTHON_EXECUTABLE "/usr/bin/python3")
set(PYTHON_INCLUDE_DIR "/usr/include/python3")
set(PYTHON_LIBRARIES "/usr/lib/x86_64-linux-gnu/libpython3.so")

include_directories(${PYTHON_INCLUDE_DIRS})
include_directories(${PYTHON_DIRECTORIES})
include_directories(/usr/local/include/yaml-cpp)
target_link_libraries(JasmineGraphLib PRIVATE ${PYTHON_LIBRARIES})
target_link_libraries(JasmineGraphLib PRIVATE m)
target_link_libraries(JasmineGraphLib PRIVATE /usr/local/lib/libkubernetes.so)
target_link_libraries(JasmineGraphLib PRIVATE yaml-cpp)
target_link_libraries(JasmineGraphLib PRIVATE curl)

if (CMAKE_BUILD_TYPE STREQUAL "DEBUG")
    # Include google test
    include(FetchContent)
    FetchContent_Declare(
            googletest
            DOWNLOAD_EXTRACT_TIMESTAMP true
            URL https://github.com/google/googletest/archive/refs/tags/release-1.12.0.zip
    )
    # For Windows: Prevent overriding the parent project's compiler/linker settings
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)
    include_directories(${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR})
    add_subdirectory(tests/unit)
    enable_testing()

    # Enable code coverage calculation
    set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake_modules)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --coverage -g -O0")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} --coverage")
    include(CodeCoverage)
    append_coverage_compiler_flags_to_target(JasmineGraphLib)
    setup_target_for_coverage_gcovr_xml(
            NAME coverage
            EXECUTABLE JasmineGraphUnitTest
            BASE_DIRECTORY "${PROJECT_SOURCE_DIR}/src"
    )
endif ()
