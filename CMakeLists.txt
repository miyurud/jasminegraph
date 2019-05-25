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
        src/partitioner/local/RDFParser.cpp src/partitioner/local/RDFParser.h src/partitioner/stream/Partitioner.cpp src/partitioner/stream/JasmineGraphIncrementalStore.cpp
        src/server/JasmineGraphInstanceService.cpp src/server/JasmineGraphInstanceService.h src/util/PlacesToNodeMapper.cpp src/util/PlacesToNodeMapper.h
        src/localstore/JasmineGraphLocalStoreFactory.cpp src/localstore/JasmineGraphLocalStoreFactory.h src/query/algorithms/triangles/Triangles.cpp src/query/algorithms/triangles/Triangles.h
        src/centralstore/JasmineGraphHashMapCentralStore.cpp src/centralstore/JasmineGraphHashMapCentralStore.h src/server/JasmineGraphInstanceService.h src/trainer/python-c-api/Python_C_API.cpp src/trainer/python-c-api/Python_C_API.h
        src/trainer/JasminGraphTrainingInitiator.cpp src/trainer/JasminGraphTrainingInitiator.h src/trainer/JasminGraphLinkPredictor.h src/trainer/JasminGraphLinkPredictor.cpp)

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
target_link_libraries(JasmineGraph cppkafka)

include_directories("$ENV{HOME}/software/flatbuffers/include")
include_directories("$ENV{HOME}/software/xerces/include")
include_directories("$ENV{HOME}/software/spdlog/include")
include_directories("$ENV{HOME}/software/jsoncpp/include")

target_link_libraries(JasmineGraph /usr/local/lib/libmetis.a)
target_link_libraries(JasmineGraph $ENV{HOME}/software/xerces-c-3.2.2/lib/libxerces-c.so)
target_link_libraries(JasmineGraph $ENV{HOME}/software/flatbuffers/libflatbuffers.a)

set(PYTHON_EXECUTABLE "/usr/bin/python3.5")
set(PYTHON_INCLUDE_DIR "/usr/include/python3.5m")
set(PYTHON_LIBRARIES "/usr/lib/x86_64-linux-gnu/libpython3.5m.so")

include_directories(${PYTHON_INCLUDE_DIRS})
include_directories(${PYTHON_DIRECTORIES})
target_link_libraries(JasmineGraph ${PYTHON_LIBRARIES})
