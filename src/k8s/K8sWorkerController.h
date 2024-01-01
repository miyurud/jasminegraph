
#ifndef JASMINEGRAPH_K8SWORKERCONTROLLER_H
#define JASMINEGRAPH_K8SWORKERCONTROLLER_H

extern "C" {
#include <kubernetes/api/AppsV1API.h>
}

#include "./K8sInterface.h"
#include "../metadb/SQLiteDBInterface.h"


class K8sWorkerController {
private:
    K8sInterface *interface;
    SQLiteDBInterface metadb;

    void spawnWorker(int workerId);

    void deleteWorker(int workerId);

    int attachExistingWorkers();

public:
    std::string masterIp;
    int numberOfWorkers;

    K8sWorkerController(std::string masterIp, int numberOfWorkers, SQLiteDBInterface *metadb);

    ~K8sWorkerController();
};

#endif //JASMINEGRAPH_K8SWORKERCONTROLLER_H

