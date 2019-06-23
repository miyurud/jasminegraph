cmake_minimum_required(VERSION 3.10)
project(JasmineGraph LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 11)

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
        src/backend/JasmineGraphBackendProtocol.cpp src/backend/JasmineGraphBackendProtocol.h src/server/JasmineGraphInstanceProtocol.cpp
        src/server/JasmineGraphInstanceProtocol.h src/server/JasmineGraphInstanceFileTransferService.cpp src/server/JasmineGraphInstanceFileTransferService.h
        src/server/JasmineGraphInstanceService.cpp src/server/JasmineGraphInstanceService.h
        src/partitioner/local/RDFParser.cpp src/partitioner/local/RDFParser.h src/partitioner/stream/Partitioner.cpp src/partitioner/stream/JasmineGraphIncrementalStore.cpp)

target_compile_options(JasmineGraph PRIVATE -std=c++11)

if(CMAKE_ENABLE_DEBUG)
    message(STATUS "DEBUG enabled")
    target_compile_options(JasmineGraph PRIVATE -g)
endif()

#TODO: Need to install the library separately
target_link_libraries(JasmineGraph sqlite3)
target_link_libraries(JasmineGraph pthread)
target_link_libraries(JasmineGraph rdkafka)
target_link_libraries(JasmineGraph cppkafka)
target_link_libraries(JasmineGraph flatbuffers)
target_link_libraries(JasmineGraph xerces-c)
find_package(spdlog CONFIG REQUIRED)
