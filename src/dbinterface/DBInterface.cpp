
#include "DBInterface.h"
#include "../util/logger/Logger.h"

Logger interface_logger;

int DBInterface::finalize() {
    return sqlite3_close(database);
}

typedef vector<vector<pair<string, string>>> table_type;

static int callback(void *ptr, int argc, char **argv, char **azColName) {
    int i;
    table_type *dbResults = static_cast<table_type *>(ptr);
    vector<pair<string, string>> results;

    for (i = 0; i < argc; i++) {
        results.push_back(make_pair(azColName[i], argv[i] ? argv[i] : "NULL"));
        // printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
    }
    dbResults->push_back(results);
    return 0;
}

vector<vector<pair<string, string>>> DBInterface::runSelect(string query) {
    char *zErrMsg = 0;
    vector<vector<pair<string, string>>> dbResults;

    if (sqlite3_exec(database, query.c_str(), callback, &dbResults, &zErrMsg) != SQLITE_OK) {
        interface_logger.log("SQL Error: " + string(zErrMsg) + " " + query, "error");
        sqlite3_free(zErrMsg);
    } else {
        interface_logger.log("Operation done successfully", "info");
    }
    return dbResults;
}

// This function inserts a new row to the DB and returns the last inserted row id
// returns -1 on error
int DBInterface::runInsert(std::string query) {
    char *zErrMsg = 0;
    int rc = sqlite3_exec(database, query.c_str(), NULL, NULL, &zErrMsg);
    if (rc != SQLITE_OK) {
        interface_logger.log("SQL Error: " + string(zErrMsg) + " " + query, "error");
        sqlite3_free(zErrMsg);
        return -1;
    }
    interface_logger.log("Insert operation done successfully", "info");
    vector<vector<pair<string, string>>> dbResults;
    string q2 = "SELECT last_insert_rowid();";

    int rc2 = sqlite3_exec(database, q2.c_str(), callback, &dbResults, &zErrMsg);

    if (rc2 != SQLITE_OK) {
        interface_logger.log("SQL Error: " + string(zErrMsg) + " " + query, "error");
        sqlite3_free(zErrMsg);
        return -1;
    }
    return std::stoi(dbResults[0][0].second);
}

// This function inserts one or more rows of the DB and nothing is returned
// This is used for inserting tables which do not have primary IDs
void DBInterface::runInsertNoIDReturn(std::string query) {
    char *zErrMsg = 0;
    int rc = sqlite3_exec(database, query.c_str(), NULL, NULL, &zErrMsg);
    if (rc != SQLITE_OK) {
        interface_logger.log("SQL Error: " + string(zErrMsg) + " " + query, "error");
        sqlite3_free(zErrMsg);
    } else {
        interface_logger.log("Insert operation done successfully: " + query, "info");
    }
}

// This function updates one or more rows of the DB
void DBInterface::runUpdate(std::string query) {
    char *zErrMsg = 0;

    int rc = sqlite3_exec(database, query.c_str(), NULL, NULL, &zErrMsg);

    if (rc != SQLITE_OK) {
        interface_logger.log("SQL Error: " + string(zErrMsg) + " " + query, "error");
        sqlite3_free(zErrMsg);
    } else {
        interface_logger.log("Update operation done successfully: " + query, "info");
    }
}

int DBInterface::RunSqlNoCallback(const char *zSql) {
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(database, zSql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) return rc;

    int rowCount = 0;
    rc = sqlite3_step(stmt);
    while (rc != SQLITE_DONE && rc != SQLITE_OK) {
        rowCount++;
        int colCount = sqlite3_column_count(stmt);
        for (int colIndex = 0; colIndex < colCount; colIndex++) {
            int type = sqlite3_column_type(stmt, colIndex);
            const char *columnName = sqlite3_column_name(stmt, colIndex);
            if (type == SQLITE_INTEGER) {
                int valInt = sqlite3_column_int(stmt, colIndex);
                printf("columnName = %s, Integer val = %d", columnName, valInt);
            } else if (type == SQLITE_FLOAT) {
                double valDouble = sqlite3_column_double(stmt, colIndex);
                printf("columnName = %s,Double val = %f", columnName, valDouble);
            } else if (type == SQLITE_TEXT) {
                const unsigned char *valChar = sqlite3_column_text(stmt, colIndex);
                printf("columnName = %s,Text val = %s", columnName, valChar);
                // free((void *) valChar);
            } else if (type == SQLITE_BLOB) {
                printf("columnName = %s,BLOB", columnName);
            } else if (type == SQLITE_NULL) {
                printf("columnName = %s,NULL", columnName);
            }
        }
        printf("Line %d, rowCount = %d", rowCount, colCount);

        rc = sqlite3_step(stmt);
    }

    rc = sqlite3_finalize(stmt);

    return rc;
}