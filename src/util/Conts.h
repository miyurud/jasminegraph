/**
Copyright 2019 JasmineGraph Team
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

#ifndef JASMINEGRAPH_CONTS_H
#define JASMINEGRAPH_CONTS_H


#include <string>

class Conts {
public:
    std::string BATCH_UPLOAD_FILE_LIST = "conf/batch-upload.txt";
    std::string JASMINEGRAPH_SERVER_PROPS_FILE = "conf/acacia-server.properties";
    std::string JASMINEGRAPH_SERVER_PUBLIC_HOSTS_FILE = "machines_public.txt";
    std::string JASMINEGRAPH_SERVER_PRIVATE_HOSTS_FILE = "machines.txt";
    static std::string JASMINEGRAPH_EXECUTABLE;
    static std::string JASMINEGRAPH_HOME;
    static std::string GRAPH_TYPE_RDF ;
    static std::string GRAPH_TYPE_NORMAL ;
    static std::string GRAPH_TYPE_NORMAL_REFORMATTED;
    static std::string GRAPH_WITH_TEXT_ATTRIBUTES;
    static std::string GRAPH_WITH_JSON_ATTRIBUTES;
    static std::string GRAPH_WITH_XML_ATTRIBUTES;

    static std::string GRAPH_WITH_ATTRIBUTES;       //To identify that there are additional attribute files to be uploaded through workers

    struct GRAPH_WITH {
        static std::string TEXT_ATTRIBUTES;         //Graph is uploaded with edge list and a plain text attribute file
        static std::string JSON_ATTRIBUTES;         //Graph is uploaded with edge list and a JSON formatted attribute file
        static std::string XML_ATTRIBUTES;          //Graph is uploaded with edge list and a XML formatted attribute file
    };


    int JASMINEGRAPH_PARTITION_INDEX_PORT;
    static int JASMINEGRAPH_FRONTEND_PORT;
    static int JASMINEGRAPH_BACKEND_PORT;
    static int JASMINEGRAPH_VERTEXCOUNTER_PORT;
    static int JASMINEGRAPH_INSTANCE_PORT;
    static int JASMINEGRAPH_INSTANCE_DATA_PORT;
    static int JASMINEGRAPH_RUNTIME_PROFILE_MASTER;
    static int JASMINEGRAPH_RUNTIME_PROFILE_WORKER;
    static int JASMINEGRAPH_WORKER_ACKNOWLEDGEMENT_TIMEOUT;
    static int COMPOSITE_CENTRAL_STORE_WORKER_THRESHOLD;
    static int NUMBER_OF_COMPOSITE_CENTRAL_STORES;
    static int RDF_NUM_OF_ATTRIBUTES;

    static int GRAPH_TYPE_TEXT;



    struct GRAPH_STATUS {
        static const int LOADING;           //Graph partitions are being uploaded
        static const int OPERATIONAL;       //Graph is uploaded and all its partitions are accessible in the current hosts setting
        static const int DELETING;          //Graph partitions are being deleted
        static const int NONOPERATIONAL;    //Graph is uploaded but some partitions of it are not accessible with the current set of active hosts
    };

    struct TRAIN_STATUS {
        static const std::string TRAINED;
        static const std::string NOT_TRAINED;
    };

    struct FLAGS {
        static const std::string GRAPH_ID;
        static const std::string LEARNING_RATE;
        static const std::string BATCH_SIZE;
        static const std::string VALIDATE_ITER;
        static const std::string EPOCHS;
    };


};


#endif //JASMINEGRAPH_CONTS_H
