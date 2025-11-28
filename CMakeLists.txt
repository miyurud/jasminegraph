cmake_minimum_required(VERSION 3.10)
project(JasmineGraph)
set(CMAKE_CXX_STANDARD 17)

# Set build type for VTune profiling if not specified
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "RelWithDebInfo")
    message(STATUS "Build type not specified, setting to RelWithDebInfo for VTune profiling")
endif()

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
find_package(Protobuf REQUIRED)
find_package(CURL REQUIRED)

set(HEADERS globals.h
        src/backend/JasmineGraphBackend.h
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
        src/partitioner/stream/Partition.h
        src/partitioner/stream/Partitioner.h
        src/performance/metrics/PerformanceUtil.h
        src/performance/metrics/StatisticCollector.h
        src/performancedb/PerformanceSQLiteDBInterface.h
        src/query/algorithms/linkprediction/JasminGraphLinkPredictor.h
        src/query/algorithms/triangles/Triangles.h
        src/query/algorithms/triangles/StreamingTriangles.h
        src/query/processor/cypher/astbuilder/ASTBuilder.h
        src/query/processor/cypher/astbuilder/ASTInternalNode.h
        src/query/processor/cypher/astbuilder/ASTLeafNoValue.h
        src/query/processor/cypher/astbuilder/ASTNode.h
        src/query/processor/cypher/astbuilder/ASTLeafValue.h
        src/query/processor/cypher/semanticanalyzer/SemanticAnalyzer.h
        src/query/processor/cypher/semanticanalyzer/Scope.h
        src/query/processor/cypher/semanticanalyzer/ScopeManager.h
        src/scale/scaler.h
        src/server/JasmineGraphInstance.h
        src/server/JasmineGraphInstanceFileTransferService.h
        src/server/JasmineGraphInstanceProtocol.h
        src/server/JasmineGraphInstanceService.h
        src/server/JasmineGraphServer.h
        src/util/Conts.h
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
        src/util/hdfs/HDFSConnector.h
        src/util/hdfs/HDFSStreamHandler.h
        src/partitioner/stream/HDFSMultiThreadedHashPartitioner.h
        src/query/processor/cypher/util/Const.h
        src/util/telemetry/OpenTelemetryUtil.h
        src/nativestore/MetaPropertyLink.h
        src/nativestore/MetaPropertyEdgeLink.h
        src/query/processor/cypher/queryplanner/Operators.h
        src/query/processor/cypher/queryplanner/QueryPlanner.h
        src/query/processor/cypher/runtime/InstanceHandler.h
        src/query/processor/cypher/runtime/OperatorExecutor.h
        src/query/processor/cypher/util/SharedBuffer.h
        src/nativestore/MetaPropertyLink.h
        src/nativestore/MetaPropertyEdgeLink.h
        src/query/processor/cypher/runtime/Helpers.h
        src/frontend/core/executor/impl/CypherQueryExecutor.h
        src/query/processor/cypher/runtime/Aggregation.h
        src/query/processor/cypher/runtime/AggregationFactory.h
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
        src/partitioner/stream/Partition.cpp
        src/partitioner/stream/Partitioner.cpp
        src/performance/metrics/PerformanceUtil.cpp
        src/performance/metrics/StatisticCollector.cpp
        src/performancedb/PerformanceSQLiteDBInterface.cpp
        src/query/algorithms/linkprediction/JasminGraphLinkPredictor.cpp
        src/query/algorithms/triangles/Triangles.cpp
        src/query/algorithms/triangles/StreamingTriangles.cpp
        src/query/processor/cypher/astbuilder/ASTBuilder.cpp
        src/query/processor/cypher/astbuilder/ASTInternalNode.cpp
        src/query/processor/cypher/astbuilder/ASTLeafNoValue.cpp
        src/query/processor/cypher/astbuilder/ASTNode.cpp
        src/query/processor/cypher/astbuilder/ASTLeafValue.cpp
        src/query/processor/cypher/semanticanalyzer/SemanticAnalyzer.cpp
        src/query/processor/cypher/semanticanalyzer/Scope.cpp
        src/query/processor/cypher/semanticanalyzer/ScopeManager.cpp
        src/scale/scaler.cpp
        src/server/JasmineGraphInstance.cpp
        src/server/JasmineGraphInstanceFileTransferService.cpp
        src/server/JasmineGraphInstanceProtocol.cpp
        src/server/JasmineGraphInstanceService.cpp
        src/server/JasmineGraphServer.cpp
        src/util/Conts.cpp
        src/util/Utils.cpp
        src/util/kafka/KafkaCC.cpp
        src/util/kafka/StreamHandler.cpp
        src/util/kafka/InstanceStreamHandler.cpp
        src/util/logger/Logger.cpp
        src/util/scheduler/SchedulerService.cpp
        src/util/telemetry/OpenTelemetryUtil.cpp
        src/util/telemetry/TelemetryInitializer.cpp
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
        src/util/hdfs/HDFSConnector.cpp
        src/util/hdfs/HDFSStreamHandler.cpp
        src/partitioner/stream/HDFSMultiThreadedHashPartitioner.cpp
        src/query/processor/cypher/semanticanalyzer/SemanticAnalyzer.cpp
        src/query/processor/cypher/util/Const.cpp
        src/frontend/ui/JasmineGraphFrontEndUIProtocol.h
        src/frontend/ui/JasmineGraphFrontEndUIProtocol.cpp
        src/frontend/ui/JasmineGraphFrontEndUI.cpp
        src/frontend/ui/JasmineGraphFrontEndUI.h
        src/frontend/core/common/JasmineGraphFrontendCommon.cpp
        src/frontend/core/common/JasmineGraphFrontendCommon.h
        src/query/processor/cypher/queryplanner/Operators.cpp
        src/query/processor/cypher/queryplanner/QueryPlanner.cpp
        src/query/processor/cypher/runtime/InstanceHandler.cpp
        src/query/processor/cypher/runtime/OperatorExecutor.cpp
        src/query/processor/cypher/util/SharedBuffer.cpp
        src/nativestore/MetaPropertyLink.cpp
        src/nativestore/MetaPropertyEdgeLink.cpp
        src/nativestore/MetaPropertyLink.cpp
        src/nativestore/MetaPropertyEdgeLink.cpp
        src/query/processor/cypher/runtime/Helpers.cpp
        src/frontend/core/executor/impl/CypherQueryExecutor.cpp
        src/query/processor/cypher/runtime/Aggregation.cpp
        src/query/processor/cypher/runtime/AggregationFactory.cpp

)

if (CMAKE_BUILD_TYPE STREQUAL "DEBUG")
    add_compile_options(-DUNIT_TEST)
    set(CMAKE_ENABLE_DEBUG ON)
endif ()

# Check for ANTLR generated files
file(GLOB GENERATED_SRC /home/ubuntu/software/antlr/*.cpp)
if(NOT GENERATED_SRC)
    message(WARNING "No ANTLR generated files found in /home/ubuntu/software/antlr/")
    set(GENERATED_SRC "")
endif()

add_library(JasmineGraphLib ${HEADERS} ${SOURCES} ${GENERATED_SRC})
add_executable(JasmineGraph main.h main.cpp)

target_compile_definitions(JasmineGraphLib PUBLIC ROOT_DIR="${CMAKE_CURRENT_SOURCE_DIR}/")

if (CMAKE_ENABLE_DEBUG)
    message(STATUS "DEBUG mode enabled")
    # Additional debug options for DEBUG builds
    target_compile_options(JasmineGraphLib PRIVATE -g)
    target_compile_options(JasmineGraph PRIVATE -g)

    # Add comprehensive debug symbols for VTune profiling
    target_compile_options(JasmineGraph PRIVATE -g3 -fno-omit-frame-pointer)
    target_compile_options(JasmineGraphLib PRIVATE -g3 -fno-omit-frame-pointer)

    # Use DWARF-4 format for better compatibility with VTune
    target_compile_options(JasmineGraph PRIVATE -gdwarf-4)
    target_compile_options(JasmineGraphLib PRIVATE -gdwarf-4)

    # Additional VTune profiling flags for Docker containers
    target_compile_options(JasmineGraph PRIVATE -fasynchronous-unwind-tables)
    target_compile_options(JasmineGraphLib PRIVATE -fasynchronous-unwind-tables)

    # Ensure debug symbols are preserved in the final binary
    target_link_options(JasmineGraph PRIVATE -g -rdynamic)
    target_link_options(JasmineGraphLib PRIVATE -g -rdynamic)

    # Force inclusion of all symbols in the binary
    target_link_options(JasmineGraph PRIVATE -Wl,--export-dynamic)
    target_link_options(JasmineGraphLib PRIVATE -Wl,--export-dynamic)

    # Disable symbol stripping for ALL build types
    set_target_properties(JasmineGraph PROPERTIES
            LINK_FLAGS_RELEASE "-g -rdynamic"
            LINK_FLAGS_RELWITHDEBINFO "-g -rdynamic"
            LINK_FLAGS_MINSIZEREL "-g -rdynamic"
    )
    set_target_properties(JasmineGraphLib PROPERTIES
            LINK_FLAGS_RELEASE "-g -rdynamic"
            LINK_FLAGS_RELWITHDEBINFO "-g -rdynamic"
            LINK_FLAGS_MINSIZEREL "-g -rdynamic"
    )

    # Ensure the executable links with the library and depends on it being built first
    add_dependencies(JasmineGraph JasmineGraphLib)

    # VTune profiling specific settings
    # Export dynamic symbol table for better function name resolution
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -rdynamic")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -rdynamic")

    # Disable dead code elimination to preserve all symbols
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-function-sections -fno-data-sections")

    # Additional flags to ensure debug symbols are preserved
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g3 -O0")
    set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -g3")

else ()
    SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-s,--gc-sections")
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
target_link_libraries(JasmineGraphLib PRIVATE /usr/local/libhdfs3/lib/libhdfs3.so)
target_link_libraries(JasmineGraph PRIVATE JasmineGraphLib)
target_link_libraries(JasmineGraph PRIVATE curl)

include_directories(/usr/local/libhdfs3/include/hdfs/)
include_directories(/usr/include/oneapi/)
include_directories(/usr/local/parmetis/include/)
include_directories(/usr/lib/x86_64-linux-gnu/openmpi/include/)
include_directories(/usr/local/include/antlr4-runtime)
link_directories(/usr/local/lib)
include_directories(/usr/local/include/yaml-cpp)
target_link_libraries(JasmineGraphLib PRIVATE m)
target_link_libraries(JasmineGraphLib PRIVATE /usr/local/lib/libkubernetes.so)
target_link_libraries(JasmineGraphLib PRIVATE yaml-cpp)
target_link_libraries(JasmineGraphLib PRIVATE curl)
target_link_libraries(JasmineGraphLib PRIVATE antlr4-runtime)

# Option to enable/disable OpenTelemetry
option(ENABLE_OPENTELEMETRY "Enable OpenTelemetry support" ON)

# Disable OpenTelemetry for DEBUG builds to avoid issues in unit tests
if(CMAKE_BUILD_TYPE STREQUAL "DEBUG")
    set(ENABLE_OPENTELEMETRY OFF)
    message(STATUS "DEBUG build detected - disabling OpenTelemetry to prevent test issues")
endif()

# Conditionally find and link OpenTelemetry packages
if(ENABLE_OPENTELEMETRY)
    # Find OpenTelemetry packages
    find_package(opentelemetry-cpp REQUIRED)
    find_package(PkgConfig REQUIRED)

    # Find nlohmann_json package for OpenTelemetry
    find_package(nlohmann_json REQUIRED)

    # OpenTelemetry libraries for native tracing support (without gRPC)
    target_link_libraries(JasmineGraphLib PRIVATE opentelemetry-cpp::api)
    target_link_libraries(JasmineGraphLib PRIVATE opentelemetry-cpp::sdk)
    target_link_libraries(JasmineGraphLib PRIVATE opentelemetry-cpp::trace)
    target_link_libraries(JasmineGraphLib PRIVATE opentelemetry-cpp::metrics)
    target_link_libraries(JasmineGraphLib PRIVATE opentelemetry-cpp::ostream_span_exporter)
    target_link_libraries(JasmineGraphLib PRIVATE opentelemetry-cpp::otlp_http_exporter)
    target_link_libraries(JasmineGraphLib PRIVATE nlohmann_json::nlohmann_json)

    # Link main executable with OpenTelemetry libraries
    target_link_libraries(JasmineGraph PRIVATE opentelemetry-cpp::api)
    target_link_libraries(JasmineGraph PRIVATE opentelemetry-cpp::sdk)
    target_link_libraries(JasmineGraph PRIVATE opentelemetry-cpp::trace)
    target_link_libraries(JasmineGraph PRIVATE opentelemetry-cpp::metrics)
    target_link_libraries(JasmineGraph PRIVATE opentelemetry-cpp::ostream_span_exporter)
    target_link_libraries(JasmineGraph PRIVATE opentelemetry-cpp::otlp_http_exporter)
    target_link_libraries(JasmineGraph PRIVATE nlohmann_json::nlohmann_json)
    
    message(STATUS "OpenTelemetry support enabled - linking OpenTelemetry libraries")
else()
    message(STATUS "OpenTelemetry support disabled - skipping OpenTelemetry library linking")
endif()

# Include OpenTelemetry headers
target_include_directories(JasmineGraphLib PRIVATE /usr/local/include)


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
