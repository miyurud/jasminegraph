//
// Created by miyurud on 9/22/18.
//

#ifndef JASMINGRAPH_JASMINGRAPHFRONTENDPROTOCOL_H
#define JASMINGRAPH_JASMINGRAPHFRONTENDPROTOCOL_H


#include <iostream>

using namespace std;

extern const string ADGR;
extern const string ADRDF;
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


class JasminGraphFrontEndProtocol {
    //Note that this protocol do not need a handshake session since the communication in most of the time is conducted
    //between JasminGraph and Humans.
    //The commands ending with -send are asking the graph id to be sent. The precommand.
};



#endif //JASMINGRAPH_JASMINGRAPHFRONTENDPROTOCOL_H
