/*
 * Copyright 2021 JasminGraph Team
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <arpa/inet.h>
#include <netdb.h>
#include <stdio.h>
#include <sys/socket.h>
#include <unistd.h>
#include <iostream>
#include <string.h>

#ifndef WORKER_DATA_PUBLISHER
#define WORKER_DATA_PUBLISHER

class DataPublisher {
   private:
    int sock = 0, valread, worker_port;
    struct sockaddr_in serv_addr;
    std::string worker_address, message;
    char buffer[1024] = {0};

   public:
    DataPublisher(int, std::string);
    void publish(std::string);
};

#endif  // !Worker_data_publisher
