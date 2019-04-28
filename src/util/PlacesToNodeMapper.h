//
// Created by chinthaka on 4/20/19.
//

#ifndef JASMINEGRAPH_PLACESTONODEMAPPER_H
#define JASMINEGRAPH_PLACESTONODEMAPPER_H


#include <string>
#include "Utils.h"


class PlacesToNodeMapper {

private:
    static int numberOfWorkers;
public:
    static std::string getHost(long placeId);
    static std::vector<int> getInstancePort(long placeId);
    static std::vector<int> getFileTransferServicePort(long placeId);
};


#endif //JASMINEGRAPH_PLACESTONODEMAPPER_H
