cmake_minimum_required(VERSION 3.10)
project(JasmineGraph)

set(CMAKE_CXX_STANDARD 11)
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -pthread")
link_libraries('sqlite3')
link_libraries('rdkafka')
link_libraries('cppkafka')
#TODO: Need to install the library separately
include_directories("$ENV{HOME}/software/flatbuffers/include")
include_directories("$ENV{HOME}/software/xerces-c-3.2.2/include")
include_directories("$ENV{HOME}/software/spdlog/include")


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
        src/partitioner/local/RDFParser.cpp src/partitioner/local/RDFParser.h)

#file(GLOB_RECURSE Dir1_Sources "*.cpp")
#add_executable(JesminGraph ${Dir1_Sources})

#target_link_libraries(JasmineGraph /usr/local/lib/libmetis.so)
#TODO: Need to install the library separately
target_link_libraries(JasmineGraph /usr/local/lib/libmetis.a)
target_link_libraries(JasmineGraph $ENV{HOME}/software/xerces-c-3.2.2/lib/libxerces-c.so)
target_link_libraries(JasmineGraph $ENV{HOME}/software/flatbuffers/libflatbuffers.a)