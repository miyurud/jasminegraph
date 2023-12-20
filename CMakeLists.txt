cmake_minimum_required(VERSION 3.10)
project(JasmineGraph LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 11)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

if (CMAKE_BUILD_TYPE STREQUAL "DEBUG")
    # Include google test
    include(FetchContent)
    FetchContent_Declare(
            googletest
            URL https://github.com/google/googletest/archive/refs/tags/release-1.12.0.zip
            DOWNLOAD_EXTRACT_TIMESTAMP true
    )
    # For Windows: Prevent overriding the parent project's compiler/linker settings
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)
    include_directories(${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR})
    add_subdirectory(tests/unit)
    enable_testing()
endif ()

set(HEADERS src/backend/JasmineGraphBackend.h
        src/backend/JasmineGraphBackendProtocol.h
        src/centralstore/JasmineGraphHashMapCentralStore.h
        src/centralstore/JasmineGraphHashMapDuplicateCentralStore.h
        src/exception/JasmineGraphException.h
        src/frontend/core/CoreConstants.h
        src/frontend/core/domain/JobRequest.h
        src/frontend/core/domain/JobResponse.h
        src/frontend/core/executor/AbstractExecutor.h
        src/frontend/core/executor/impl/TriangleCountExecutor.h
        src/frontend/core/factory/ExecutorFactory.h
        src/frontend/core/scheduler/JobScheduler.h
        src/localstore/JasmineGraphLocalStore.h
        src/localstore/JasmineGraphHashMapLocalStore.h
        src/localstore/JasmineGraphLocalStoreFactory.h
        src/metadb/SQLiteDBInterface.h
        src/ml/trainer/JasmineGraphTrainingSchedular.h
        src/partitioner/local/MetisPartitioner.h
        src/partitioner/local/JSONParser.h
        src/partitioner/local/RDFPartitioner.h
        src/partitioner/local/RDFParser.h
        src/performance/metrics/PerformanceUtil.h
        src/performance/metrics/StatisticCollector.h
        src/performancedb/PerformanceSQLiteDBInterface.h
        src/query/algorithms/linkprediction/JasminGraphLinkPredictor.h
        src/query/algorithms/triangles/Triangles.h
        src/server/JasmineGraphServer.h
        src/server/JasmineGraphInstance.h
        src/server/JasmineGraphInstanceProtocol.h
        src/server/JasmineGraphInstanceFileTransferService.h
        src/server/JasmineGraphInstanceService.h
        src/frontend/JasmineGraphFrontEnd.h
        src/frontend/JasmineGraphFrontEndProtocol.h
        src/util/Utils.h
        src/util/Conts.h
        src/util/PlacesToNodeMapper.h
        src/util/kafka/KafkaCC.h
        src/util/logger/Logger.h
        src/util/scheduler/Scheduler.h
        src/util/scheduler/InterruptableSleep.h
        src/util/scheduler/Cron.h
        src/util/scheduler/ctpl_stl.h
        src/util/scheduler/SchedulerService.h
        src/util/dbutil/edgestore_generated.h
        src/util/dbutil/attributestore_generated.h
        src/util/dbutil/partedgemapstore_generated.h)

set(SOURCES src/backend/JasmineGraphBackend.cpp
        src/backend/JasmineGraphBackendProtocol.cpp
        src/centralstore/incremental/DataPublisher.cpp
        src/centralstore/incremental/NodeBlock.cpp
        src/centralstore/incremental/NodeManager.cpp
        src/centralstore/incremental/PropertyLink.cpp
        src/centralstore/incremental/RelationBlock.cpp
        src/centralstore/JasmineGraphHashMapCentralStore.cpp
        src/centralstore/JasmineGraphHashMapDuplicateCentralStore.cpp
        src/exception/JasmineGraphException.cpp
        src/frontend/core/CoreConstants.cpp
        src/frontend/core/domain/JobRequest.cpp
        src/frontend/core/domain/JobResponse.cpp
        src/frontend/core/executor/AbstractExecutor.cpp
        src/frontend/core/executor/impl/TriangleCountExecutor.cpp
        src/frontend/core/factory/ExecutorFactory.cpp
        src/frontend/core/scheduler/JobScheduler.cpp
        src/frontend/JasmineGraphFrontEnd.cpp
        src/frontend/JasmineGraphFrontEndProtocol.cpp
        src/localstore/incremental/JasmineGraphIncrementalLocalStore.cpp
        src/localstore/JasmineGraphHashMapLocalStore.cpp
        src/localstore/JasmineGraphLocalStore.cpp
        src/localstore/JasmineGraphLocalStoreFactory.cpp
        src/metadb/SQLiteDBInterface.cpp
        src/ml/trainer/JasmineGraphTrainingSchedular.cpp
        src/partitioner/local/MetisPartitioner.cpp
        src/partitioner/local/JSONParser.cpp
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
        src/server/JasmineGraphInstance.cpp
        src/server/JasmineGraphInstanceFileTransferService.cpp
        src/server/JasmineGraphInstanceProtocol.cpp
        src/server/JasmineGraphInstanceService.cpp
        src/server/JasmineGraphServer.cpp
        src/util/Conts.cpp
        src/util/kafka/KafkaCC.cpp
        src/util/logger/Logger.cpp
        src/util/PlacesToNodeMapper.cpp
        src/util/scheduler/SchedulerService.cpp
        src/util/Utils.cpp)

add_library(JasmineGraphLib ${HEADERS} ${SOURCES})
add_executable(JasmineGraph main.h main.cpp)

target_compile_definitions(JasmineGraphLib PUBLIC ROOT_DIR="${CMAKE_CURRENT_SOURCE_DIR}/")

if (CMAKE_ENABLE_DEBUG)
    message(STATUS "DEBUG enabled")
    target_compile_options(JasmineGraph PRIVATE -g)
endif ()

#TODO: Need to install the library separately
target_link_libraries(JasmineGraphLib sqlite3)
target_link_libraries(JasmineGraphLib pthread)
target_link_libraries(JasmineGraphLib rdkafka)
target_link_libraries(JasmineGraphLib Threads::Threads)
target_link_libraries(JasmineGraphLib fmt)
target_link_libraries(JasmineGraphLib /usr/lib/x86_64-linux-gnu/libxerces-c.so)
target_link_libraries(JasmineGraphLib /usr/lib/x86_64-linux-gnu/libflatbuffers.a)
target_link_libraries(JasmineGraphLib /usr/lib/x86_64-linux-gnu/libjsoncpp.so)
target_link_libraries(JasmineGraphLib /usr/local/lib/libcppkafka.so)
target_link_libraries(JasmineGraph JasmineGraphLib)
target_include_directories(JasmineGraph PRIVATE /usr/include/python3.8)

set(PYTHON_EXECUTABLE "/usr/bin/python3.8")
set(PYTHON_INCLUDE_DIR "/usr/include/python3.8m")
set(PYTHON_LIBRARIES "/usr/lib/x86_64-linux-gnu/libpython3.8.so")

include_directories(${PYTHON_INCLUDE_DIRS})
include_directories(${PYTHON_DIRECTORIES})
target_link_libraries(JasmineGraphLib ${PYTHON_LIBRARIES})
target_link_libraries(JasmineGraphLib m)
