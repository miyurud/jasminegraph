//
// Created by chinthaka on 8/24/19.
//

#include "PerformanceUtil.h"
#include "../../backend/JasmineGraphBackendProtocol.h"

Utils systemUtils;

int PerformanceUtil::reportPerformanceStatistics() {
    int memoryUsage = getVirtualMemoryUsage();
    return 0;
}

int PerformanceUtil::getVirtualMemoryUsage() {
    FILE* file = fopen("/proc/self/status", "r");
    int result = -1;
    char line[128];

    while (fgets(line, 128, file) != NULL){
        if (strncmp(line, "VmSize:", 7) == 0){
            result = parseLine(line);
            break;
        }
    }
    fclose(file);
    std::cout << "Memory Usage: " + std::to_string(result) << std::endl;
    return result;


}

int PerformanceUtil::parseLine(char* line){
    int i = strlen(line);
    const char* p = line;
    while (*p <'0' || *p > '9') p++;
    line[i-3] = '\0';
    i = atoi(p);
    return i;
}

int PerformanceUtil::publishStatisticsToMaster()  {
    std::string masterHost = systemUtils.getJasmineGraphProperty("org.jasminegraph.server.host");
    int masterBackendPort = Conts::JASMINEGRAPH_BACKEND_PORT;

    int sockfd;
    char data[300];
    bool loop = false;
    socklen_t len;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        std::cerr << "Cannot accept connection" << std::endl;
        return 0;
    }

    server = gethostbyname(masterHost.c_str());
    if (server == NULL) {
        std::cerr << "ERROR, no host named " << server << std::endl;
    }

    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *) server->h_addr,
          (char *) &serv_addr.sin_addr.s_addr,
          server->h_length);
    serv_addr.sin_port = htons(masterBackendPort);
    if (connect(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
        std::cerr << "ERROR connecting" << std::endl;
    }

    bzero(data, 301);
    write(sockfd, HANDSHAKE.c_str(), HANDSHAKE.size());
    //frontend_logger.log("Sent : " + JasminGraphBackendProtocol::HANDSHAKE, "info");
    bzero(data, 301);
    read(sockfd, data, 300);
    string response = (data);

    response = systemUtils.trim_copy(response, " \f\n\r\t\v");

    if (response.compare(HANDSHAKE_OK) == 0) {

    }
}