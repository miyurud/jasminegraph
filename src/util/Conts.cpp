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

#include "Conts.h"

std::string Conts::JASMINEGRAPH_EXECUTABLE = "run.sh";
std::string Conts::JASMINEGRAPH_HOME  = "JASMINEGRAPH_HOME";

std::string Conts::GRAPH_TYPE_RDF  = "RDF_GRAPH";
std::string Conts::GRAPH_TYPE_NORMAL  = "NORMAL_GRAPH";
std::string Conts::GRAPH_TYPE_NORMAL_REFORMATTED  = "REFORMATTED_GRAPH";
std::string Conts::GRAPH_WITH_TEXT_ATTRIBUTES = "TEXT_ATT";
std::string Conts::GRAPH_WITH_JSON_ATTRIBUTES = "JSON_ATT";
std::string Conts::GRAPH_WITH_XML_ATTRIBUTES = "XML_ATT";

std::string Conts::GRAPH_WITH_ATTRIBUTES  = "GRAPH_WITH_ATTRIBUTES";



std::string Conts::GRAPH_WITH::TEXT_ATTRIBUTES = "1 : Graph with edge list + text attributes list";
std::string Conts::GRAPH_WITH::JSON_ATTRIBUTES = "2 : Graph with edge list + JSON attributes list";
std::string Conts::GRAPH_WITH::XML_ATTRIBUTES = "3 : Graph with edge list + XML attributes list";


int Conts::JASMINEGRAPH_FRONTEND_PORT = 7777;
int Conts::JASMINEGRAPH_BACKEND_PORT = 7778;
int Conts::JASMINEGRAPH_VERTEXCOUNTER_PORT = 7779;
int Conts::JASMINEGRAPH_INSTANCE_PORT = 7780;//Worker port
int Conts::JASMINEGRAPH_INSTANCE_DATA_PORT = 7781;//Data Port

int Conts::GRAPH_TYPE_TEXT= 1 ;//Data Port

int Conts::JASMINEGRAPH_WORKER_ACKNOWLEDGEMENT_TIMEOUT = 30000;

int Conts::COMPOSITE_CENTRAL_STORE_WORKER_THRESHOLD = 4;
int Conts::NUMBER_OF_COMPOSITE_CENTRAL_STORES = 4;

int Conts::JASMINEGRAPH_RUNTIME_PROFILE_MASTER = 1;
int Conts::JASMINEGRAPH_RUNTIME_PROFILE_WORKER = 2;

int Conts::RDF_NUM_OF_ATTRIBUTES = 7;


const int Conts::GRAPH_STATUS::LOADING = 1;
const int Conts::GRAPH_STATUS::OPERATIONAL = 2;
const int Conts::GRAPH_STATUS::DELETING = 3;
const int Conts::GRAPH_STATUS::NONOPERATIONAL = 4;

const std::string Conts::TRAIN_STATUS::TRAINED = "trained";
const std::string Conts::TRAIN_STATUS::NOT_TRAINED = "not_trained";

const std::string Conts::FLAGS::GRAPH_ID = "graph_id";
const std::string Conts::FLAGS::LEARNING_RATE = "learning_rate";
const std::string Conts::FLAGS::BATCH_SIZE = "batch_size";
const std::string Conts::FLAGS::VALIDATE_ITER = "validate_iter";
const std::string Conts::FLAGS::EPOCHS = "epochs";