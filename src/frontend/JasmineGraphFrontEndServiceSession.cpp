//
// Created by miyurud on 12/15/18.
//

#include "JasmineGraphFrontEndServiceSession.h"
#include "../util/Utils.h"
#include "JasmineGraphFrontEndProtocol.h"
#include "JasmineGraphFrontEnd.h"

//void *task1(void *dummyPt)
//{
//    frontendservicesessionargs* sessionargs = (frontendservicesessionargs*) dummyPt;
//    cout << "Thread No: " << pthread_self() << endl;
//    char data[300];
//    bzero(data, 301);
//    bool loop = false;
//    while(!loop)
//    {
//        bzero(data, 301);
//        read(sessionargs->connFd, data, 300);
//
//        string line (data);
//        cout << line << endl;
//
//        line = Utils::trim_copy(line, " \f\n\r\t\v");
//
//        if(line.compare(EXIT) == 0)
//        {
//            break;
//        }
//        else if (line.compare(LIST) == 0)
//        {
//            SQLiteDBInterface* sqlite = &sessionargs->sqlite;
//            vector<vector<pair<string, string> >> v = sqlite->runSelect("SELECT idgraph, name, upload_path, upload_start_time, upload_end_time, "
//                                                           "graph_status_idgraph_status, vertexcount, centralpartitioncount, edgecount FROM graph;");
//            for(vector<vector<pair<string, string> >>::iterator it = v.begin(); it != v.end(); ++it) {
//                std::cout << it->front().first << endl;
//                std::cout << it->front().second << endl;
//            }
//        }
//        else if (line.compare(SHTDN) == 0)
//        {
//            close(sessionargs->connFd);
//            exit(0);
//        }
//    }
//    cout << "\nClosing thread and connection" << endl;
//    close(sessionargs->connFd);
//}