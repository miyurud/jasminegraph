/**
Copyright 2018 JasminGraph Team
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

#include "JasminGraphFrontEnd.h"
#include "../util/Conts.h"
#include "../util/Utils.h"
#include "JasminGraphFrontEndProtocol.h"
#include "../metadb/SQLiteDBInterface.h"

using namespace std;

static int connFd;

JasminGraphFrontEnd::JasminGraphFrontEnd(SQLiteDBInterface db)
{
    this->sqlite = db;
}

int JasminGraphFrontEnd::run() {
    int pId;
    int portNo = Conts::JASMINGRAPH_FRONTEND_PORT;;
    int listenFd;
    socklen_t len;
    bool loop = false;
    struct sockaddr_in svrAdd;
    struct sockaddr_in clntAdd;

    pthread_t threadA[3];

    //create socket
    listenFd = socket(AF_INET, SOCK_STREAM, 0);

    if(listenFd < 0)
    {
        cerr << "Cannot open socket" << endl;
        return 0;
    }

    bzero((char*) &svrAdd, sizeof(svrAdd));

    svrAdd.sin_family = AF_INET;
    svrAdd.sin_addr.s_addr = INADDR_ANY;
    svrAdd.sin_port = htons(portNo);

    int yes=1;

    if (setsockopt(listenFd,SOL_SOCKET,SO_REUSEADDR,&yes,sizeof yes) == -1) {
        perror("setsockopt");
        exit(1);
    }


    //bind socket
    if(bind(listenFd, (struct sockaddr *) &svrAdd, sizeof(svrAdd)) < 0)
    {
        cerr << "Cannot bind" << endl;
        return 0;
    }

    listen(listenFd, 5);

    len = sizeof(clntAdd);

    int noThread = 0;

    while (noThread < 3)
    {
        cout << "Listening" << endl;

        //this is where client connects. svr will hang in this mode until client conn
        connFd = accept(listenFd, (struct sockaddr *)&clntAdd, &len);

        if (connFd < 0)
        {
            cerr << "Cannot accept connection" << endl;
            return 0;
        }
        else
        {
            cout << "Connection successful" << endl;
        }

        pthread_create(&threadA[noThread], NULL, task1, &this->sqlite);

        noThread++;
    }

    for(int i = 0; i < 3; i++)
    {
        pthread_join(threadA[i], NULL);
    }


}

void *task1(void *dummyPt)
{
    cout << "Thread No: " << pthread_self() << endl;
    char data[300];
    bzero(data, 301);
    bool loop = false;
    while(!loop)
    {
        bzero(data, 301);
        read(connFd, data, 300);

        string line (data);
        cout << line << endl;

        line = Utils::trim_copy(line, " \f\n\r\t\v");

        if(line.compare(EXIT) == 0)
        {
            break;
        }
        else if (line.compare(LIST) == 0)
        {
            SQLiteDBInterface* sqlite = (SQLiteDBInterface*) dummyPt;
            std::vector<std::string> v = sqlite->runSelect("SELECT idgraph, name, upload_path, upload_start_time, upload_end_time, "
                              "graph_status_idgraph_status, vertexcount, centralpartitioncount, edgecount FROM graph;");
            for(std::vector<std::string>::iterator it = v.begin(); it != v.end(); ++it) {
                std::cout << *it << endl;
            }
        }
        else if (line.compare(SHTDN) == 0)
        {
            close(connFd);
            exit(0);
        }
    }
    cout << "\nClosing thread and connection" << endl;
    close(connFd);
}