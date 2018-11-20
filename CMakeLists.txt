cmake_minimum_required(VERSION 3.10)
project(JasminGraph)

set(CMAKE_CXX_STANDARD 11)
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -pthread")
link_libraries('sqlite3')

include_directories("/home/chinthaka/research/flatbuffers/include")

add_executable(JasminGraph main.cpp src/server/JasminGraphServer.cpp src/server/JasminGraphServer.h src/metadb/SQLiteDBInterface.cpp src/util/dbutil/edgestore_generated.h src/metadb/SQLiteDBInterface.h main.h src/server/JasminGraphInstance.cpp src/server/JasminGraphInstance.h src/localstore/JasminGraphLocalStore.cpp src/localstore/JasminGraphLocalStore.h src/util/Utils.cpp src/util/Utils.h src/util/Conts.cpp src/util/Conts.h src/localstore/JasminGraphHashMapLocalStore.cpp src/localstore/JasminGraphHashMapLocalStore.h src/frontend/JasminGraphFrontEnd.h src/frontend/JasminGraphFrontEnd.cpp src/frontend/JasminGraphFrontEndProtocol.h src/frontend/JasminGraphFrontEndProtocol.cpp src/localstore/JasminGraphHashMapLocalStore.cpp src/localstore/JasminGraphHashMapLocalStore.h src/partitioner/local/MetisPartitioner.cpp src/partitioner/local/MetisPartitioner.h)

target_link_libraries(JasminGraph /usr/local/lib/libmetis.so)

target_link_libraries(JasminGraph /usr/local/lib/libmetis.a)
