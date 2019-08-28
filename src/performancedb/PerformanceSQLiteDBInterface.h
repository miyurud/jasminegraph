//
// Created by chinthaka on 8/26/19.
//

#ifndef JASMINEGRAPH_PERFORMANCESQLITEDBINTERFACE_H
#define JASMINEGRAPH_PERFORMANCESQLITEDBINTERFACE_H

#include "../util/sqlite3/sqlite3.h"
#include <vector>
#include <map>


class PerformanceSQLiteDBInterface {
private:
    sqlite3 *database;
public:
    int init();

    int finalize();

    std::vector<std::vector<std::pair<std::string, std::string>>> runSelect(std::string);

    int runInsert(std::string);

    void runUpdate(std::string);

    void runInsertNoIDReturn(std::string);

    int RunSqlNoCallback(const char *zSql);

    PerformanceSQLiteDBInterface();
};


#endif //JASMINEGRAPH_PERFORMANCESQLITEDBINTERFACE_H
