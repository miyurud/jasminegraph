/**
Copyright 2019 JasminGraph Team
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

#ifndef JASMINGRAPH_JASMINGRAPHFRONTENDPROTOCOL_H
#define JASMINGRAPH_JASMINGRAPHFRONTENDPROTOCOL_H


#include <iostream>

using namespace std;

extern const string ADGR;
extern const string ADRDF;
extern const string ADGR_CUST;
extern const string RMGR;
extern const string TRUNCATE;
extern const string DONE;
extern const string ENVI;
extern const string RUOK;
extern const string IMOK;
extern const string EXIT;
extern const string EXIT_ACK;
extern const string SHTDN;
extern const string LIST;
extern const string VCOUNT;
extern const string ECOUNT;
extern const string SEND;
extern const string ERROR;
extern const string DEBUG;
extern const string GREM;
extern const string GREM_ACK;
extern const string GREM_SEND;
extern const string GREM_DONE;
extern const string PAGERANK;
extern const string OUT_DEGREE;
extern const string IN_DEGREE;
extern const string IN_DEGREE_SEND;
extern const string AVERAGE_OUT_DEGREE;
extern const string AVERAGE_IN_DEGREE;
extern const string TOP_K_PAGERANK;
extern const string TOP_K_SEND;
extern const string FREE_DATA_DIR_SPACE;
extern const string GRAPHID_SEND;
extern const string TRIANGLES;
extern const string K_CORE;
extern const string K_VALUE;
extern const string K_NN;
extern const string SPARQL;
extern const string S_QUERY_SEND;
extern const string OUTPUT_FILE_NAME;
extern const string OUTPUT_FILE_PATH;
extern const string ADD_STREAM;
extern const string ADD_STREAM_KAFKA;
extern const string STRM_ACK;
extern const string ADD_STREAM_KAFKA;
extern const string STREAM_TOPIC_NAME;
extern const string REFORMAT;
extern const string TRAIN;
extern const string PREDICT;


class JasminGraphFrontEndProtocol {
    //Note that this protocol do not need a handshake session since the communication in most of the time is conducted
    //between JasminGraph and Humans.
    //The commands ending with -send are asking the graph id to be sent. The precommand.
};

const int FRONTEND_COMMAND_LENGTH = 4;


#endif //JASMINGRAPH_JASMINGRAPHFRONTENDPROTOCOL_H
