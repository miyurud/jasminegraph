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

#include <iostream>
#include <map>
#include "JasmineGraphServer.h"
#include "../metadb/SQLiteDBInterface.h"
#include "JasmineGraphInstance.h"
#include "../frontend/JasmineGraphFrontEnd.h"
#include "../util/Utils.h"
#include "../partitioner/local/MetisPartitioner.h"
#include "JasmineGraphInstanceProtocol.h"
#include "../util/logger/Logger.h"

Logger server_logger;

struct workers{
    std::string hostname;
    int port;
    int dataPort;
};


static std::vector<workers> hostWorkerMap;

void *runfrontend(void *dummyPt) {
    JasmineGraphServer *refToServer = (JasmineGraphServer *) dummyPt;
    refToServer->frontend = new JasmineGraphFrontEnd(refToServer->sqlite);
    refToServer->frontend->run();
}

void *runbackend(void *dummyPt) {
    JasmineGraphServer *refToServer = (JasmineGraphServer *) dummyPt;
    refToServer->backend = new JasmineGraphBackend(refToServer->sqlite);
    refToServer->backend->run();
}


JasmineGraphServer::JasmineGraphServer() {

}

JasmineGraphServer::~JasmineGraphServer() {
    puts("Freeing up server resources.");
    sqlite.finalize();
}

int JasmineGraphServer::run() {
    server_logger.log("Running the server...", "info");

    this->sqlite = *new SQLiteDBInterface();
    this->sqlite.init();
    init();
    start_workers();
    return 0;
}

bool JasmineGraphServer::isRunning() {
    return true;
}

void JasmineGraphServer::init() {
    Utils utils;
    std::map<string, string> result = utils.getBatchUploadFileList(
            utils.getJasmineGraphProperty("org.jasminegraph.batchupload.file.path"));

    if (result.size() != 0) {
        std::map<std::string, std::string>::iterator iterator1 = result.begin();
        while (iterator1 != result.end()) {
            std::string fileName = iterator1->first;
            std::string filePath = iterator1->second;
            //Next, we need to implement the batch upload logic here.
            iterator1++;
        }
    }

    pthread_t frontendthread;
    pthread_t backendthread;
    pthread_create(&frontendthread, NULL, runfrontend, this);
    pthread_create(&backendthread, NULL, runbackend, this);
}

void JasmineGraphServer::start_workers() {
    Utils utils;
    int hostListModeNWorkers = 0;
    int numberOfWorkersPerHost;
    std::vector<std::string> hostsList = utils.getHostList();
    std::string nWorkers = utils.getJasmineGraphProperty("org.jasminegraph.server.nworkers");
    int workerPort = Conts::JASMINEGRAPH_INSTANCE_PORT;
    int workerDataPort = Conts::JASMINEGRAPH_INSTANCE_DATA_PORT;
    if (utils.is_number(nWorkers)) {
        numberOfWorkers = atoi(nWorkers.c_str());
    } else {
        server_logger.log("Number of Workers is not specified", "error");
        numberOfWorkers = 0;
    }

    if (numberOfWorkers > 0 && hostsList.size() > 0) {
        numberOfWorkersPerHost = hostsList.size()/numberOfWorkers;
        hostListModeNWorkers = hostsList.size() % numberOfWorkers;
    }

    std::vector<std::string>::iterator it;
    it = hostsList.begin();

    for (it = hostsList.begin(); it < hostsList.end(); it++) {
        std::string item = *it;
        int portCount = 0;
        std::vector<int> portVector = workerPortsMap[item];
        std::vector<int> dataPortVector = workerDataPortsMap[item];

        while (portCount < numberOfWorkersPerHost) {
            portVector.push_back(workerPort);
            dataPortVector.push_back(workerDataPort);
            hostWorkerMap.push_back({*it,workerPort,workerDataPort});
            workerPort = workerPort + 2;
            workerDataPort = workerDataPort + 2;
            portCount ++;
        }

        if (hostListModeNWorkers > 0) {
            portVector.push_back(workerPort);
            dataPortVector.push_back(workerDataPort);
            hostWorkerMap.push_back({*it,workerPort,workerDataPort});
            workerPort = workerPort + 2;
            workerDataPort = workerDataPort + 2;
            hostListModeNWorkers--;
        }

        workerPortsMap[item] = portVector;
        workerDataPortsMap[item] = dataPortVector;

    }

    int hostListSize = hostsList.size();
    std::vector<std::string>::iterator hostListIterator;
    hostListIterator = hostsList.begin();


    std::thread* myThreads = new std::thread[hostListSize];
    int count =0;

    for (hostListIterator = hostsList.begin(); hostListIterator < hostsList.end(); hostListIterator++) {
        std::string host = *hostListIterator;
        myThreads[count] = std::thread(startRemoteWorkers,workerPortsMap[host],workerDataPortsMap[host], host);
        count++;
    }

    for (int threadCount=0;threadCount<hostListSize;threadCount++) {
        myThreads[threadCount].join();
        std::cout<<"############JOINED###########"<< std::endl;
    }

}


void JasmineGraphServer::startRemoteWorkers(std::vector<int> workerPortsVector,
                                                    std::vector<int> workerDataPortsVector, std::string host) {
    Utils utils;
    std::string workerPath = utils.getJasmineGraphProperty("org.jasminegraph.worker.path");
    std::string artifactPath = utils.getJasmineGraphProperty("org.jasminegraph.artifact.path");
    std::string jasmineGraphExecutableName = Conts::JASMINEGRAPH_EXECUTABLE;
    std::string executableFile = workerPath+"/"+jasmineGraphExecutableName;
    std::string serverStartScript;
    char buffer[128];
    std::string result = "";

    if (artifactPath.empty() || artifactPath.find_first_not_of (' ') == artifactPath.npos) {
        artifactPath = utils.getJasmineGraphHome();
    }

    for (int i =0 ; i < workerPortsVector.size() ; i++) {
        if (host.find("localhost") != std::string::npos) {
            copyArtifactsToWorkers(workerPath,artifactPath,host);
            serverStartScript = executableFile+" 2 "+ std::to_string(workerPortsVector.at(i)) + " " + std::to_string(workerDataPortsVector.at(i));
        } else {
            copyArtifactsToWorkers(workerPath,artifactPath,host);
            serverStartScript = "ssh -p 22 " + host+ " "+ executableFile + " 2"+" "+ std::to_string(workerPortsVector.at(i)) + " " + std::to_string(workerDataPortsVector.at(i));
        }
        //std::cout<<serverStartScript<< std::endl;
        FILE *input = popen(serverStartScript.c_str(),"r");

        if (input) {
            // read the input
            while (!feof(input)) {
                if (fgets(buffer, 128, input) != NULL) {
                    result.append(buffer);
                }
            }
            if (!result.empty()) {
                std::cout<<result<< std::endl;
            }
            pclose(input);
        }
    }
}

void JasmineGraphServer::uploadGraphLocally(int graphID) {
    std::cout << "Uploading the graph locally.." << std::endl;
    std::vector<string> partitionFileList = MetisPartitioner::getPartitionFiles();
    int count = 0;
    std::thread* workerThreads = new std::thread[hostWorkerMap.size()];
    std::vector<workers, std::allocator<workers>>::iterator mapIterator;
    for (mapIterator = hostWorkerMap.begin(); mapIterator < hostWorkerMap.end(); mapIterator++) {
        workers worker = *mapIterator;
        workerThreads[count] = std::thread(batchUploadFile,worker.hostname,worker.port, worker.dataPort,graphID, partitionFileList[count]  );
        count++;
    }

    for (int threadCount=0;threadCount<count;threadCount++) {
        workerThreads[threadCount].join();
    }

    //TODO::Update the database as required
}

bool JasmineGraphServer::batchUploadFile(std::string host, int port, int dataPort, int graphID, std::string filePath) {
    Utils utils;
    bool result = true;
    std::cout <<pthread_self()<< " host : " <<host<< " port : " <<port<< " DPort : " << dataPort<<std::endl;
    int listenFd;
    char data[300];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in svrAdd;
    struct sockaddr_in clntAdd;

    //create socket
    listenFd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenFd < 0) {
        std::cerr << "Cannot open socket" << std::endl;
        return 0;
    }

    bzero(data, 301);
    bzero((char *) &svrAdd, sizeof(svrAdd));

    svrAdd.sin_family = AF_INET;
    svrAdd.sin_addr.s_addr = INADDR_ANY;
    svrAdd.sin_port = htons(port);

    int yes = 1;

    if (setsockopt(listenFd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes) == -1) {
        perror("setsockopt");
        exit(1);
    }


    //bind socket
    if (bind(listenFd, (struct sockaddr *) &svrAdd, sizeof(svrAdd)) < 0) {
        std::cerr << "Cannot bind" << std::endl;
        return 0;
    }

    listen(listenFd, 5);

    len = sizeof(clntAdd);

    int connFd = accept(listenFd, (struct sockaddr *) &clntAdd, &len);

    if (connFd < 0) {
        std::cerr << "Cannot accept connection" << std::endl;
        return 0;
    } else {
        std::cout << "Connection successful" << std::endl;
    }

    std::cout << JasmineGraphInstanceProtocol::HANDSHAKE << std::endl;
    write(connFd, JasmineGraphInstanceProtocol::HANDSHAKE.c_str(), JasmineGraphInstanceProtocol::HANDSHAKE.size());

    bzero(data, 301);
    read(connFd, data, 300);
    string response = (data);

    std::cout << response << std::endl;

    response = utils.trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) == 0) {
        std::cout << host << std::endl;
        write(connFd, host.c_str(), host.size());
    }

    std::cout << JasmineGraphInstanceProtocol::BATCH_UPLOAD << std::endl;
    write(connFd, JasmineGraphInstanceProtocol::BATCH_UPLOAD.c_str(), JasmineGraphInstanceProtocol::BATCH_UPLOAD.size());

    bzero(data, 301);
    read(connFd, data, 300);
    response = (data);
    response = utils.trim_copy(response, " \f\n\r\t\v");
    std::cout << response << std::endl;
    if (response.compare(JasmineGraphInstanceProtocol::OK) == 0) {
        std::cout << graphID << std::endl;
        write(connFd, std::to_string(graphID).c_str(), std::to_string(graphID).size());

        std::string fileName = utils.getFileName(filePath);
        std::string fileLength = to_string(utils.getFileSize(filePath));

        bzero(data, 301);
        read(connFd, data, 300);
        response = (data);
        response = utils.trim_copy(response, " \f\n\r\t\v");

        if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_NAME) == 0) {
            std::cout << fileName << std::endl;
            write(connFd, fileName.c_str(), fileName.size());

            bzero(data, 301);
            read(connFd, data, 300);
            response = (data);
            response = utils.trim_copy(response, " \f\n\r\t\v");

            if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_LEN) == 0) {
                std::cout << fileLength << std::endl;
                write(connFd, fileLength.c_str(), fileLength.size());

                bzero(data, 301);
                read(connFd, data, 300);
                response = (data);
                response = utils.trim_copy(response, " \f\n\r\t\v");

                if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE_CONT) == 0) {
                    sendFileThroughService(host, dataPort, filePath, filePath);
                }
            }
        }
        int count = 0;

        while (true) {
            std::cout << JasmineGraphInstanceProtocol::FILE_RECV_CHK << std::endl;
            write(connFd, JasmineGraphInstanceProtocol::FILE_RECV_CHK.c_str(), JasmineGraphInstanceProtocol::FILE_RECV_CHK.size());

            bzero(data, 301);
            read(connFd, data, 300);
            response = (data);
            response = utils.trim_copy(response, " \f\n\r\t\v");

            if (response.compare(JasmineGraphInstanceProtocol::FILE_RECV_WAIT) == 0) {
                std::cout << "Checking file status : " << count << std::endl;
                count++;
                //Thread.currentThread().sleep(1000);
                //We sleep for 1 second, and try again.
                continue;
            }
            else{
                break;
            }
        }

        std::cout << "File transfer completed..." << std::endl;

        //Next we wait till the batch upload completes
        while(true){

            std::cout << JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK << std::endl;
            write(connFd, JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.c_str(), JasmineGraphInstanceProtocol::BATCH_UPLOAD_CHK.size());

            bzero(data, 301);
            read(connFd, data, 300);
            response = (data);
            response = utils.trim_copy(response, " \f\n\r\t\v");

            if (response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_WAIT) == 0) {
                //Thread.currentThread().sleep(1000);
                //We sleep for 1 second, and try again.
                continue;
            }
            else{
                break;
            }
        }

        if(response.compare(JasmineGraphInstanceProtocol::BATCH_UPLOAD_ACK) == 0){
            std::cout << "Batch upload completed..." << std::endl;
        }else{
            std::cout << "There was an error in the upload process..." << std::endl;
        }

    }
    close(connFd);
    return result;
}

bool JasmineGraphServer::sendFileThroughService(std::string host, int dataPort, std::string fileName,
                                                std::string filePath) {
    Utils utils;
    int listenFd;
    char data[300];
    socklen_t len;
    struct sockaddr_in svrAdd;
    struct sockaddr_in clntAdd;

    //create socket
    listenFd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenFd < 0) {
        std::cerr << "Cannot open socket" << std::endl;
        return 0;
    }

    bzero(data, 301);
    bzero((char *) &svrAdd, sizeof(svrAdd));

    svrAdd.sin_family = AF_INET;
    svrAdd.sin_addr.s_addr = INADDR_ANY;
    svrAdd.sin_port = htons(dataPort);

    int yes = 1;

    if (setsockopt(listenFd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes) == -1) {
        perror("setsockopt");
        exit(1);
    }


    //bind socket
    if (bind(listenFd, (struct sockaddr *) &svrAdd, sizeof(svrAdd)) < 0) {
        std::cerr << "Cannot bind" << std::endl;
        return 0;
    }

    listen(listenFd, 5);

    len = sizeof(clntAdd);

    thread_local int connFd = accept(listenFd, (struct sockaddr *) &clntAdd, &len);

    if (connFd < 0) {
        std::cerr << "Cannot accept connection" << std::endl;
        return 0;
    } else {
        std::cout << "Connection successful" << std::endl;
    }

    write(connFd, fileName.c_str(), fileName.size());

    bzero(data, 301);
    read(connFd, data, 300);
    string response = (data);

    std::cout << response << std::endl;

    response = utils.trim_copy(response, " \f\n\r\t\v");

    if (response.compare(JasmineGraphInstanceProtocol::SEND_FILE) == 0) {
        std::cout << "Sending file" << std::endl;
        // TODO : Send file through socket
        std::string dataFolderPath = utils.getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
        char buffer[128];
        std::string result = "";
        string serverStartScript = "scp -P " + to_string(dataPort) + " " + filePath + " " + host + ":" + dataFolderPath;
        std::cout << "Script: " << serverStartScript << std::endl;
        FILE *input = popen(serverStartScript.c_str(), "r");

        if (input) {
            // read the input
            while (!feof(input)) {
                if (fgets(buffer, 128, input) != NULL) {
                    result.append(buffer);
                }
            }
            if (!result.empty()) {
                std::cout << result << std::endl;
            }
            pclose(input);
        }

    }
}

void JasmineGraphServer::copyArtifactsToWorkers(std::string workerPath, std::string artifactLocation,
                                                      std::string remoteWorker) {
    std::string pathCheckCommand = "test -e " + workerPath + "&& echo file exists || echo file not found";
    std::string artifactCopyCommand;
    std::string localWorkerArtifactCopyCommand = "cp -r "+ artifactLocation+"/* "+workerPath;
    std::string remoteWorkerArtifactCopyCommand = "scp -r " + artifactLocation + "/* " + remoteWorker + ":" + workerPath;

    char buffer[128];
    std::string result = "";

    if (remoteWorker.find("localhost") == std::string::npos) {
        std::string remotePathCheckCommand = "ssh -p 22 " + remoteWorker+ " " +  pathCheckCommand;
        pathCheckCommand = remotePathCheckCommand;
    }

    FILE *input = popen(pathCheckCommand.c_str(),"r");

    if (input) {
        // read the input
        while (!feof(input)) {
            if (fgets(buffer, 128, input) != NULL) {
                result.append(buffer);
            }
        }
        if (!result.empty() && remoteWorker.find("file not found") == std::string::npos) {
            createWorkerPath(remoteWorker,workerPath);
        }
        pclose(input);
    }

    if (remoteWorker.find("localhost") != std::string::npos) {
        artifactCopyCommand = localWorkerArtifactCopyCommand;
    } else {
        artifactCopyCommand = remoteWorkerArtifactCopyCommand;
    }

    FILE *copyInput = popen(artifactCopyCommand.c_str(),"r");

    if (copyInput) {
        // read the input
        while (!feof(copyInput)) {
            if (fgets(buffer, 128, copyInput) != NULL) {
                result.append(buffer);
            }
        }
        if (!result.empty()) {
            std::cout<<result<< std::endl;
        }
        pclose(copyInput);
    }
}

void JasmineGraphServer::createWorkerPath(std::string workerHost, std::string workerPath) {
    std::string pathCreationCommand = "mkdir -p " + workerPath;
    char buffer[128];
    std::string result = "";

    if (workerHost.find("localhost") == std::string::npos) {
        std::string tmpPathCreation = pathCreationCommand;
        pathCreationCommand = "ssh -p 22 " + workerHost+ " " + tmpPathCreation;
    }

    FILE *input = popen(pathCreationCommand.c_str(),"r");

    if (input) {
        // read the input
        while (!feof(input)) {
            if (fgets(buffer, 128, input) != NULL) {
                result.append(buffer);
            }
        }
        if (!result.empty()) {
            std::cout<<result<< std::endl;
        }
        pclose(input);
    }
}
