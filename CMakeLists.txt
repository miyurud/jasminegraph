cmake_minimum_required(VERSION 3.10)
project(JasmineGraph LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 11)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

add_executable(JasmineGraph main.cpp src/server/JasmineGraphServer.cpp src/server/JasmineGraphServer.h src/metadb/SQLiteDBInterface.cpp
        src/util/dbutil/edgestore_generated.h src/util/dbutil/attributestore_generated.h src/util/dbutil/partedgemapstore_generated.h src/metadb/SQLiteDBInterface.h main.h src/server/JasmineGraphInstance.cpp
        src/server/JasmineGraphInstance.h src/localstore/JasmineGraphLocalStore.cpp src/localstore/JasmineGraphLocalStore.h
        src/util/Utils.cpp src/util/Utils.h src/util/kafka/KafkaCC.h src/util/kafka/KafkaCC.cpp src/util/Conts.cpp src/util/Conts.h src/localstore/JasmineGraphHashMapLocalStore.cpp
        src/localstore/JasmineGraphHashMapLocalStore.h src/frontend/JasmineGraphFrontEnd.h src/frontend/JasmineGraphFrontEnd.cpp
        src/frontend/JasmineGraphFrontEndProtocol.h src/frontend/JasmineGraphFrontEndProtocol.cpp
        src/localstore/JasmineGraphHashMapLocalStore.cpp src/localstore/JasmineGraphHashMapLocalStore.h
        src/partitioner/local/MetisPartitioner.cpp src/partitioner/local/MetisPartitioner.h src/partitioner/local/RDFPartitioner.cpp
        src/partitioner/local/JSONParser.cpp src/partitioner/local/JSONParser.h
        src/partitioner/local/RDFPartitioner.h src/backend/JasmineGraphBackend.cpp src/backend/JasmineGraphBackend.h
        src/backend/JasmineGraphBackendProtocol.cpp src/backend/JasmineGraphBackendProtocol.h src/util/logger/Logger.h src/util/logger/Logger.cpp
        src/server/JasmineGraphInstanceProtocol.cpp
        src/centralstore/incremental/NodeManager.cpp
        src/centralstore/incremental/NodeBlock.cpp
        src/centralstore/incremental/PropertyLink.cpp
        src/centralstore/incremental/RelationBlock.cpp
        src/centralstore/incremental/DataPublisher.cpp
        src/partitioner/stream/Partition.cpp
        src/localstore/incremental/JasmineGraphIncrementalLocalStore.cpp
        src/server/JasmineGraphInstanceProtocol.h src/server/JasmineGraphInstanceFileTransferService.cpp src/server/JasmineGraphInstanceFileTransferService.h
        src/partitioner/local/RDFParser.cpp src/partitioner/local/RDFParser.h src/partitioner/stream/Partitioner.cpp src/partitioner/stream/JasmineGraphIncrementalStore.cpp
        src/server/JasmineGraphInstanceService.cpp src/server/JasmineGraphInstanceService.h src/util/PlacesToNodeMapper.cpp src/util/PlacesToNodeMapper.h
        src/localstore/JasmineGraphLocalStoreFactory.cpp src/localstore/JasmineGraphLocalStoreFactory.h src/query/algorithms/triangles/Triangles.cpp src/query/algorithms/triangles/Triangles.h
        src/centralstore/JasmineGraphHashMapCentralStore.cpp src/centralstore/JasmineGraphHashMapCentralStore.h src/util/scheduler/Scheduler.h src/util/scheduler/InterruptableSleep.h src/util/scheduler/Cron.h src/util/scheduler/ctpl_stl.h src/util/performance/PerformanceUtil.cpp src/util/performance/PerformanceUtil.h src/util/scheduler/SchedulerService.cpp src/util/scheduler/SchedulerService.h src/performancedb/PerformanceSQLiteDBInterface.cpp src/performancedb/PerformanceSQLiteDBInterface.h src/util/performance/StatisticCollector.cpp src/util/performance/StatisticCollector.h
        src/ml/trainer/python-c-api/Python_C_API.cpp src/ml/trainer/python-c-api/Python_C_API.h
        src/ml/trainer/JasmineGraphTrainingSchedular.cpp src/ml/trainer/JasmineGraphTrainingSchedular.h src/ml/trainer/JasminGraphTrainingInitiator.cpp src/ml/trainer/JasminGraphTrainingInitiator.h src/query/algorithms/linkprediction/JasminGraphLinkPredictor.h src/query/algorithms/linkprediction/JasminGraphLinkPredictor.cpp src/centralstore/JasmineGraphHashMapDuplicateCentralStore.cpp src/centralstore/JasmineGraphHashMapDuplicateCentralStore.h src/exception/JasmineGraphException.cpp src/exception/JasmineGraphException.h)

target_compile_options(JasmineGraph PRIVATE -std=c++11 -I/usr/include/python3.5)

if(CMAKE_ENABLE_DEBUG)
    message(STATUS "DEBUG enabled")
    target_compile_options(JasmineGraph PRIVATE -g)
endif()

#target_link_libraries(JasmineGraph /usr/local/lib/libmetis.so)
#target_link_libraries(JasmineGraph /usr/local/lib/libxerces-c-3.2.so)
#TODO: Need to install the library separately
target_link_libraries(JasmineGraph sqlite3)
target_link_libraries(JasmineGraph pthread)
target_link_libraries(JasmineGraph rdkafka)
#target_link_libraries(JasmineGraph cppkafka)
target_link_libraries(JasmineGraph Threads::Threads)
include_directories("$ENV{JASMINEGRAPH_HOME}/software/flatbuffers/include")
include_directories("$ENV{JASMINEGRAPH_HOME}/software/xerces/include")
include_directories("$ENV{JASMINEGRAPH_HOME}/software/spdlog/include")
include_directories("$ENV{JASMINEGRAPH_HOME}/software/jsoncpp/include")
include_directories("$ENV{JASMINEGRAPH_HOME}/software/cppkafka/include")
include_directories("$ENV{JASMINEGRAPH_HOME}/software/nlohmann_json/include/nlohmann")

target_link_libraries(JasmineGraph /usr/local/lib/libmetis.a)
target_link_libraries(JasmineGraph $ENV{JASMINEGRAPH_HOME}/software/xerces-c-3.2.2/lib/libxerces-c.so)
target_link_libraries(JasmineGraph $ENV{JASMINEGRAPH_HOME}/software/flatbuffers/libflatbuffers.a)
include_directories("$ENV{JASMINEGRAPH_HOME}/software/xerces-c-3.2.2/include")
target_link_libraries(JasmineGraph $ENV{JASMINEGRAPH_HOME}/software/jsoncpp/src/lib_json/libjsoncpp.a)
target_link_libraries(JasmineGraph $ENV{JASMINEGRAPH_HOME}/software/cppkafka/build/src/lib/libcppkafka.so)

set(PYTHON_EXECUTABLE "/usr/bin/python3.6")
set(PYTHON_INCLUDE_DIR "/usr/include/python3.6m")
set(PYTHON_LIBRARIES "/usr/lib/x86_64-linux-gnu/libpython3.6m.so")

include_directories(${PYTHON_INCLUDE_DIRS})
include_directories(${PYTHON_DIRECTORIES})
target_link_libraries(JasmineGraph ${PYTHON_LIBRARIES})
target_link_libraries(JasmineGraph m)