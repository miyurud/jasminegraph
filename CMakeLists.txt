cmake_minimum_required(VERSION 3.10)
project(JasmineGraph LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 11)
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -g -pthread -I/usr/include/python3.5")

link_libraries('sqlite3')
link_libraries('rdkafka')
link_libraries('cppkafka')
#TODO: Need to install the library separately
include_directories("$ENV{HOME}/software/flatbuffers/include")
include_directories("$ENV{HOME}/software/xerces-c-3.2.2/include")
include_directories("$ENV{HOME}/software/spdlog/include")

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

add_executable(JasmineGraph main.cpp
        src/server/JasmineGraphServer.cpp src/server/JasmineGraphServer.h src/metadb/SQLiteDBInterface.cpp src/util/dbutil/edgestore_generated.h
        src/util/dbutil/attributestore_generated.h src/util/dbutil/partedgemapstore_generated.h src/metadb/SQLiteDBInterface.h main.h
        src/server/JasmineGraphInstance.cpp src/server/JasmineGraphInstance.h src/localstore/JasmineGraphLocalStore.cpp src/localstore/JasmineGraphLocalStore.h
        src/util/Utils.cpp src/util/Utils.h src/util/kafka/KafkaCC.h src/util/kafka/KafkaCC.cpp
        src/util/Conts.cpp src/util/Conts.h src/localstore/JasmineGraphHashMapLocalStore.cpp src/localstore/JasmineGraphHashMapLocalStore.h
        src/frontend/JasmineGraphFrontEnd.h src/frontend/JasmineGraphFrontEnd.cpp src/frontend/JasmineGraphFrontEndProtocol.h src/frontend/JasmineGraphFrontEndProtocol.cpp
        src/partitioner/local/MetisPartitioner.cpp src/partitioner/local/MetisPartitioner.h src/partitioner/local/RDFPartitioner.cpp
            src/partitioner/local/JSONParser.cpp src/partitioner/local/JSONParser.h
        src/partitioner/local/RDFPartitioner.h src/backend/JasmineGraphBackend.cpp src/backend/JasmineGraphBackend.h
        src/partitioner/local/JSONParser.cpp src/partitioner/local/JSONParser.h src/partitioner/local/RDFPartitioner.h src/backend/JasmineGraphBackend.cpp src/backend/JasmineGraphBackend.h
        src/backend/JasmineGraphBackendProtocol.cpp src/backend/JasmineGraphBackendProtocol.h src/util/logger/Logger.h src/util/logger/Logger.cpp
        src/backend/JasmineGraphBackendProtocol.cpp src/backend/JasmineGraphBackendProtocol.h src/server/JasmineGraphInstanceProtocol.cpp
        src/server/JasmineGraphInstanceProtocol.h src/server/JasmineGraphInstanceFileTransferService.cpp src/server/JasmineGraphInstanceFileTransferService.h
        src/partitioner/local/RDFParser.cpp src/partitioner/local/RDFParser.h src/partitioner/stream/Partitioner.cpp src/partitioner/stream/JasmineGraphIncrementalStore.cpp
        src/server/JasmineGraphInstanceService.cpp src/server/JasmineGraphInstanceService.h src/util/PlacesToNodeMapper.cpp src/util/PlacesToNodeMapper.h
        src/localstore/JasmineGraphLocalStoreFactory.cpp src/localstore/JasmineGraphLocalStoreFactory.h src/query/algorithms/triangles/Triangles.cpp src/query/algorithms/triangles/Triangles.h
        src/centralstore/JasmineGraphHashMapCentralStore.cpp src/centralstore/JasmineGraphHashMapCentralStore.h src/scheduler/Scheduler.h src/scheduler/InterruptableSleep.h src/scheduler/Cron.h src/scheduler/ctpl_stl.h src/util/performance/PerformanceUtil.cpp src/util/performance/PerformanceUtil.h src/scheduler/SchedulerService.cpp src/scheduler/SchedulerService.h src/performancedb/PerformanceSQLiteDBInterface.cpp src/performancedb/PerformanceSQLiteDBInterface.h src/util/performance/StatisticCollector.cpp src/util/performance/StatisticCollector.h
        src/server/JasmineGraphInstanceProtocol.cpp src/server/JasmineGraphInstanceProtocol.h src/server/JasmineGraphInstanceFileTransferService.cpp src/server/JasmineGraphInstanceFileTransferService.h
        src/server/JasmineGraphInstanceService.cpp src/server/JasmineGraphInstanceService.h
        src/partitioner/local/RDFParser.cpp src/partitioner/local/RDFParser.h src/partitioner/stream/Partitioner.cpp src/partitioner/stream/JasmineGraphIncrementalStore.cpp
        src/trainer/python-c-api/Python_C_API.cpp src/trainer/python-c-api/Python_C_API.h
        src/trainer/JasminGraphTrainingInitiator.cpp src/trainer/JasminGraphTrainingInitiator.h src/trainer/JasminGraphLinkPredictor.h src/trainer/JasminGraphLinkPredictor.cpp)

target_compile_options(JasmineGraph PRIVATE -std=c++11)

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
target_link_libraries(JasmineGraph cppkafka)
target_link_libraries(JasmineGraph Threads::Threads)

include_directories("$ENV{HOME}/software/flatbuffers/include")
include_directories("$ENV{HOME}/software/xerces/include")
include_directories("$ENV{HOME}/software/spdlog/include")
include_directories("$ENV{HOME}/software/jsoncpp/include")

target_link_libraries(JasmineGraph /usr/local/lib/libmetis.a)
target_link_libraries(JasmineGraph $ENV{HOME}/software/xerces-c-3.2.2/lib/libxerces-c.so)
target_link_libraries(JasmineGraph $ENV{HOME}/software/flatbuffers/libflatbuffers.a)
target_link_libraries(JasmineGraph /opt/lib/libxerces-c.a)
target_link_libraries(JasmineGraph $ENV{HOME}/software/jsoncpp/build/debug/src/lib_json/libjsoncpp.a)

set(PYTHON_EXECUTABLE "/usr/bin/python3.5")
set(PYTHON_INCLUDE_DIR "/usr/include/python3.5m")
set(PYTHON_LIBRARIES "/usr/lib/x86_64-linux-gnu/libpython3.5m.so")

include_directories(${PYTHON_INCLUDE_DIRS})
include_directories(${PYTHON_DIRECTORIES})
target_link_libraries(JasmineGraph ${PYTHON_LIBRARIES})
