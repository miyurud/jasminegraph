/**
Copyright 2018 JasmineGraph Team
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

#include <string>
#include "SQLiteDBInterface.h"
#include "../util/Utils.h"

using namespace std;

int SQLiteDBInterface::init()
{
    char *sql;
    /* Create SQL statement */
    sql = "CREATE TABLE COMPANY("  \
         "ID INT PRIMARY KEY     NOT NULL," \
         "NAME           TEXT    NOT NULL," \
         "AGE            INT     NOT NULL," \
         "ADDRESS        CHAR(50)," \
         "SALARY         REAL );";

    Utils utils;
    int rc = sqlite3_open(utils.getJasmineGraphProperty("org.jasminegraph.db.location").c_str(), &database);

    if (rc)
    {
        fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(database));
        return(-1);
    } else {
        fprintf(stderr, "Opened database successfully\n");
    }
}

int SQLiteDBInterface::finalize(){
    sqlite3_close(database);
}

SQLiteDBInterface::SQLiteDBInterface() {

}

typedef vector<vector<pair<string, string> >> table_type;

static int callback(void *ptr, int argc, char **argv, char **azColName) {
    int i;
    table_type *dbResults = static_cast<table_type *>(ptr);
    vector<pair<string, string>> results;

    for (i = 0; i < argc; i++) {
        results.push_back(make_pair(azColName[i], argv[i] ? argv[i] : "NULL"));
        //printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
    }
    dbResults->push_back(results);
    return 0;
}

vector<vector<pair<string, string> >> SQLiteDBInterface::runSelect(std::string query) {
    char *zErrMsg = 0;
    int rc;
    vector<vector<pair<string, string>>> dbResults;

    rc = sqlite3_exec(database, query.c_str(), callback, &dbResults, &zErrMsg);

    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    } else {
        fprintf(stdout, "Operation done successfully\n");
        return dbResults;
    }
}

int SQLiteDBInterface::RunSqlNoCallback(const char * zSql)
{
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(database, zSql, -1, &stmt, NULL);
    if (rc != SQLITE_OK)
        return rc;

    int rowCount = 0;
    rc = sqlite3_step(stmt);
    while (rc != SQLITE_DONE && rc != SQLITE_OK)
    {
        rowCount++;
        int colCount = sqlite3_column_count(stmt);
        for (int colIndex = 0; colIndex < colCount; colIndex++)
        {
            int type = sqlite3_column_type(stmt, colIndex);
            const char * columnName = sqlite3_column_name(stmt, colIndex);
            if (type == SQLITE_INTEGER)
            {
                int valInt = sqlite3_column_int(stmt, colIndex);
                printf("columnName = %s, Integer val = %d", columnName, valInt);
            }
            else if (type == SQLITE_FLOAT)
            {
                double valDouble = sqlite3_column_double(stmt, colIndex);
                printf("columnName = %s,Double val = %f", columnName, valDouble);
            }
            else if (type == SQLITE_TEXT)
            {
                const unsigned char * valChar = sqlite3_column_text(stmt, colIndex);
                printf("columnName = %s,Text val = %s", columnName, valChar);
                //free((void *) valChar);
            }
            else if (type == SQLITE_BLOB)
            {
                printf("columnName = %s,BLOB", columnName);
            }
            else if (type == SQLITE_NULL)
            {
                printf("columnName = %s,NULL", columnName);
            }
        }
        printf("Line %d, rowCount = %d", rowCount, colCount);

        rc = sqlite3_step(stmt);
    }

    rc = sqlite3_finalize(stmt);

    return rc;
}
