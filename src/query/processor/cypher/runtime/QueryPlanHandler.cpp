//
// Created by kumarawansha on 12/13/24.
//

#include "QueryPlanHandler.h"

QueryPlanHandler::QueryPlanHandler(int numberOfPartitions, std::vector<DataPublisher *> &workerClients)
        : numberOfPartitions(numberOfPartitions), workerClients(workerClients){};

