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

#ifndef JASMINGRAPH_CONTS_H
#define JASMINGRAPH_CONTS_H


#include <string>

class Conts {
public:
    std::string BATCH_UPLOAD_FILE_LIST = "conf/batch-upload.txt";
    std::string JASMINGRAPH_SERVER_PROPS_FILE = "conf/acacia-server.properties";
    std::string JASMINGRAPH_SERVER_PUBLIC_HOSTS_FILE = "machines_public.txt";
    std::string JASMINGRAPH_SERVER_PRIVATE_HOSTS_FILE = "machines.txt";

    int JASMINGRAPH_PARTITION_INDEX_PORT;
    static int JASMINGRAPH_FRONTEND_PORT;
    static int JASMINGRAPH_BACKEND_PORT;
    static int JASMINGRAPH_VERTEXCOUNTER_PORT;
    static int JASMINGRAPH_INSTANCE_PORT;
    static int JASMINGRAPH_INSTANCE_DATA_PORT;
};


#endif //JASMINGRAPH_CONTS_H
