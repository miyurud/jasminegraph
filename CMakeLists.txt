cmake_minimum_required(VERSION 3.10)
project(JasmineGraph)

set(CMAKE_CXX_STANDARD 11)
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -pthread")
link_libraries('sqlite3')
#TODO: Need to install the library separately
include_directories("$ENV{HOME}/software/flatbuffers/include")

add_executable(JasmineGraph main.cpp src/server/JasmineGraphServer.cpp src/server/JasmineGraphServer.h src/metadb/SQLiteDBInterface.cpp
        src/util/dbutil/edgestore_generated.h src/metadb/SQLiteDBInterface.h main.h src/server/JasmineGraphInstance.cpp
        src/server/JasmineGraphInstance.h src/localstore/JasmineGraphLocalStore.cpp src/localstore/JasmineGraphLocalStore.h
        src/util/Utils.cpp src/util/Utils.h src/util/Conts.cpp src/util/Conts.h src/localstore/JasmineGraphHashMapLocalStore.cpp
        src/localstore/JasmineGraphHashMapLocalStore.h src/frontend/JasmineGraphFrontEnd.h src/frontend/JasmineGraphFrontEnd.cpp
        src/frontend/JasmineGraphFrontEndProtocol.h src/frontend/JasmineGraphFrontEndProtocol.cpp
        src/localstore/JasmineGraphHashMapLocalStore.cpp src/localstore/JasmineGraphHashMapLocalStore.h
        src/partitioner/local/MetisPartitioner.cpp src/partitioner/local/MetisPartitioner.h src/partitioner/local/RDFPartitioner.cpp
        src/partitioner/local/RDFPartitioner.h src/backend/JasmineGraphBackend.cpp src/backend/JasmineGraphBackend.h
        src/backend/JasmineGraphBackendProtocol.cpp src/backend/JasmineGraphBackendProtocol.h src/server/JasmineGraphInstanceProtocol.cpp
        src/server/JasmineGraphInstanceProtocol.h src/server/JasmineGraphInstanceServiceSession.h src/server/JasmineGraphInstanceServiceSession.cpp
        src/server/JasmineGraphInstanceFileTransferServiceSession.h src/server/JasmineGraphInstanceFileTransferServiceSession.cpp)

#file(GLOB_RECURSE Dir1_Sources "*.cpp")
#add_executable(JesminGraph ${Dir1_Sources})

#target_link_libraries(JasmineGraph /usr/local/lib/libmetis.so)
#TODO: Need to install the library separately
target_link_libraries(JasmineGraph /usr/local/lib/libmetis.a)
