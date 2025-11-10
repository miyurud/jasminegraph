//
// Created by sajeenthiran on 2025-08-26.
//

#include "SemanticBeamSearchExecutor.h"

#include "../../../../server/JasmineGraphServer.h"

Logger semantic_beam_search_logger_executor;

SemanticBeamSearchExecutor::SemanticBeamSearchExecutor() {}

SemanticBeamSearchExecutor::SemanticBeamSearchExecutor(
    SQLiteDBInterface *db, PerformanceSQLiteDBInterface *perfDb,
    JobRequest jobRequest) {
  this->sqlite = db;
  this->perfDB = perfDb;
  this->request = jobRequest;
}

void SemanticBeamSearchExecutor::execute() {
  int uniqueId = getUid();
  std::string masterIP = request.getMasterIP();
  std::string graphId = request.getParameter(Conts::PARAM_KEYS::GRAPH_ID);
  std::string queryString =
      request.getParameter(Conts::PARAM_KEYS::CYPHER_QUERY::QUERY_STRING);
  int numberOfPartitions =
      std::stoi(request.getParameter(Conts::PARAM_KEYS::NO_OF_PARTITIONS));
  std::string canCalibrateString =
      request.getParameter(Conts::PARAM_KEYS::CAN_CALIBRATE);
  std::string autoCalibrateString =
      request.getParameter(Conts::PARAM_KEYS::AUTO_CALIBRATION);
  std::string queueTime = request.getParameter(Conts::PARAM_KEYS::QUEUE_TIME);
  int connFd =
      std::stoi(request.getParameter(Conts::PARAM_KEYS::CONN_FILE_DESCRIPTOR));
  bool *loop_exit = reinterpret_cast<bool *>(static_cast<std::uintptr_t>(
      std::stoull(request.getParameter(Conts::PARAM_KEYS::LOOP_EXIT_POINTER))));
  const auto &workerList = JasmineGraphServer::getWorkers(numberOfPartitions);
  bool canCalibrate = Utils::parseBoolean(canCalibrateString);
  bool autoCalibrate = Utils::parseBoolean(autoCalibrateString);
  std::vector<std::future<void>> intermRes;
  std::vector<std::future<int>> statResponse;

  auto begin = chrono::high_resolution_clock::now();

  std::vector<std::unique_ptr<SharedBuffer>> bufferPool;
  bufferPool.reserve(numberOfPartitions);  // Pre-allocate space for pointers
  for (size_t i = 0; i < numberOfPartitions; ++i) {
    bufferPool.emplace_back(std::make_unique<SharedBuffer>(MASTER_BUFFER_SIZE));
  }
  std::vector<std::thread> readThreads;
  int count = 0;

  std::vector<std::thread> workerThreads;
  count = 0;
  for (auto worker : workerList) {
    workerThreads.emplace_back(doSemanticBeamSearch, worker.hostname,
                               worker.port, masterIP, std::stoi(graphId), count,
                               queryString, std::ref(*bufferPool[count]),
                               numberOfPartitions);
    count++;
  }
  vector<json> results;
  int closeFlag = 0;
  int result_wr;
  for (size_t i = 0; i < bufferPool.size(); ++i) {
    readThreads.emplace_back([&, i]() {
      semantic_beam_search_logger_executor.info(
          "Starting read thread for bufferPool[" + std::to_string(i) + "]");
      while (true) {
        std::string data = bufferPool[i]->get();
        semantic_beam_search_logger_executor.info(
            "Fetched data from bufferPool[" + std::to_string(i) + "]: " + data);
        if (data == "-1") {
          break;
        }
        results.push_back(json::parse(data));
      }
    });
  }
  for (auto &t : readThreads) {
    if (t.joinable()) {
      t.join();
    }
  }

  for (auto &thread : workerThreads) {
    if (thread.joinable()) {
      thread.join();
    }
  }

  // sort based score and trim to top k
  std::sort(results.begin(), results.end(), [](const json &a, const json &b) {
    return a["score"] > b["score"];
  });

  // trim to top k
  int k = 10;
  if ((int)results.size() > k) results.resize(k);

  // write to socket
  count = 0;
  for (const auto &res : results) {
    std::string data = res.dump();
    count++;
    result_wr = write(connFd, data.c_str(), data.length());
    result_wr = write(connFd, Conts::CARRIAGE_RETURN_NEW_LINE.c_str(),
                      Conts::CARRIAGE_RETURN_NEW_LINE.size());
    if (result_wr < 0) {
      semantic_beam_search_logger_executor.error("Error writing to socket");
      *loop_exit = true;
      break;
    }
  }
  semantic_beam_search_logger_executor.info(
      "###CYPHER-QUERY-EXECUTOR### Executing Query : Fetching Results");

  semantic_beam_search_logger_executor.info(
      "###CYPHER-QUERY-EXECUTOR### Executing Query : Completed");

  workerResponded = true;
  JobResponse jobResponse;
  jobResponse.setJobId(request.getJobId());
  responseVector.push_back(jobResponse);

  responseVectorMutex.lock();
  responseMap[request.getJobId()] = jobResponse;
  responseVectorMutex.unlock();

  auto end = chrono::high_resolution_clock::now();
  auto dur = end - begin;
  auto msDuration =
      std::chrono::duration_cast<std::chrono::milliseconds>(dur).count();

  std::string durationString = std::to_string(msDuration);

  if (canCalibrate || autoCalibrate) {
    Utils::updateSLAInformation(perfDB, graphId, numberOfPartitions, msDuration,
                                CYPHER, Conts::SLA_CATEGORY::LATENCY);
    isStatCollect = false;
  }

  processStatusMutex.lock();
  for (auto processCompleteIterator = processData.begin();
       processCompleteIterator != processData.end();
       ++processCompleteIterator) {
    ProcessInfo processInformation = *processCompleteIterator;
    if (processInformation.id == uniqueId) {
      processData.erase(processInformation);
      break;
    }
  }
  processStatusMutex.unlock();
}

void SemanticBeamSearchExecutor::doSemanticBeamSearch(
    std::string host, int port, std::string masterIP, int graphID,
    int partitionId, std::string query, SharedBuffer &sharedBuffer,
    int noOfPartitions) {
  semantic_beam_search_logger_executor.info("Connecting to worker at " + host +
                                            ":" + std::to_string(port));
  semantic_beam_search_logger_executor.debug(
      "Parameters: host=" + host + ", port=" + std::to_string(port) +
      ", masterIP=" + masterIP + ", graphID=" + std::to_string(graphID) +
      ", partitionId=" + std::to_string(partitionId) + ", query=" + query);

  int sockfd;
  struct sockaddr_in serv_addr;
  struct hostent *server;
  static const int ACK_MESSAGE_SIZE = 1024;
  char ack[ACK_MESSAGE_SIZE] = {0};

  // --- Create socket ---
  sockfd = socket(AF_INET, SOCK_STREAM, 0);
  semantic_beam_search_logger_executor.debug("Socket created, sockfd=" +
                                             std::to_string(sockfd));
  if (sockfd < 0) {
    semantic_beam_search_logger_executor.error("Cannot create socket");
    return;
  }

  // --- Resolve host ---
  if (host.find('@') != std::string::npos) {
    semantic_beam_search_logger_executor.debug(
        "Host contains '@', splitting...");
    host = Utils::split(host, '@')[1];
    semantic_beam_search_logger_executor.debug("Resolved host: " + host);
  }
  server = gethostbyname(host.c_str());
  if (server == NULL) {
    semantic_beam_search_logger_executor.error("No host named " + host);
    close(sockfd);
    return;
  }

  bzero((char *)&serv_addr, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr,
        server->h_length);
  serv_addr.sin_port = htons(port);

  semantic_beam_search_logger_executor.debug("Attempting to connect to " +
                                             host + ":" + std::to_string(port));
  if (Utils::connect_wrapper(sockfd, (struct sockaddr *)&serv_addr,
                             sizeof(serv_addr)) < 0) {
    semantic_beam_search_logger_executor.error("Connection failed to " + host +
                                               ":" + std::to_string(port));
    close(sockfd);
    return;
  }
  semantic_beam_search_logger_executor.debug("Connected to worker");

  // --- Step 1: Send command ---
  std::string command = "initiate-semantic-beam-search\r\n";
  semantic_beam_search_logger_executor.debug("Sending command: " + command);
  if (send(sockfd, command.c_str(), command.size(), 0) <= 0) {
    semantic_beam_search_logger_executor.error("Failed to send command");
    close(sockfd);
    return;
  }

  bzero(ack, ACK_MESSAGE_SIZE);
  semantic_beam_search_logger_executor.debug("Waiting for ACK after command");
  if (recv(sockfd, ack, strlen("stream-c-length-ack"), 0) <= 0) {
    semantic_beam_search_logger_executor.error(
        "Failed to receive ACK after command");
    close(sockfd);
    return;
  }
  semantic_beam_search_logger_executor.debug("Received ACK after command");

  // --- Step 2: Send graphID ---
  int length = htonl(std::to_string(graphID).size());
  semantic_beam_search_logger_executor.debug("Sending graphID length: " +
                                             std::to_string(ntohl(length)));
  send(sockfd, &length, sizeof(length), 0);
  semantic_beam_search_logger_executor.debug("Sending graphID: " +
                                             std::to_string(graphID));
  if (send(sockfd, std::to_string(graphID).c_str(),
           std::to_string(graphID).size(), 0) <= 0) {
    semantic_beam_search_logger_executor.error("Failed to send graphID");
    close(sockfd);
    return;
  }
  bzero(ack, ACK_MESSAGE_SIZE);
  semantic_beam_search_logger_executor.debug("Waiting for ACK after graphID");
  if (recv(sockfd, ack, strlen("stream-c-length-ack"), 0) <= 0) {
    semantic_beam_search_logger_executor.error(
        "Failed to receive ACK after graphID");
    close(sockfd);
    return;
  }
  semantic_beam_search_logger_executor.debug("Received ACK after graphID");

  // --- Step 3: Send partition ID ---
  length = htonl(std::to_string(partitionId).size());
  semantic_beam_search_logger_executor.debug("Sending partitionId length: " +
                                             std::to_string(ntohl(length)));
  send(sockfd, &length, sizeof(length), 0);
  semantic_beam_search_logger_executor.debug("Sending partitionId: " +
                                             std::to_string(partitionId));
  if (send(sockfd, std::to_string(partitionId).c_str(),
           std::to_string(partitionId).size(), 0) <= 0) {
    semantic_beam_search_logger_executor.error("Failed to send partitionId");
    close(sockfd);
    return;
  }
  bzero(ack, ACK_MESSAGE_SIZE);
  semantic_beam_search_logger_executor.debug(
      "Waiting for ACK after partitionId");
  if (recv(sockfd, ack, strlen("stream-c-length-ack"), 0) <= 0) {
    semantic_beam_search_logger_executor.error(
        "Failed to receive ACK after partitionId");
    close(sockfd);
    return;
  }
  semantic_beam_search_logger_executor.debug("Received ACK after partitionId");

  // --- Step 4: Send query ---
  semantic_beam_search_logger_executor.info("semantic beam searcg" + query);
  length = htonl(query.size());
  semantic_beam_search_logger_executor.debug("Sending query length: " +
                                             std::to_string(ntohl(length)));
  send(sockfd, &length, sizeof(length), 0);
  semantic_beam_search_logger_executor.debug("Sending query: " + query);
  if (send(sockfd, query.c_str(), query.size(), 0) <= 0) {
    semantic_beam_search_logger_executor.error("Failed to send query");
    close(sockfd);
    return;
  }
  bzero(ack, ACK_MESSAGE_SIZE);
  semantic_beam_search_logger_executor.debug("Waiting for ACK after query");
  if (recv(sockfd, ack, strlen("stream-c-length-ack"), 0) <= 0) {
    semantic_beam_search_logger_executor.error(
        "Failed to receive ACK after query");
    close(sockfd);
    return;
  }

  int counter = 0;
  string workers = "";
  // --- Step 4: Send workers ---
  for (JasmineGraphServer::worker worker :
       JasmineGraphServer::getWorkers(noOfPartitions)) {
    counter++;
    semantic_beam_search_logger_executor.info("count " +
                                              std::to_string(counter));
    workers += worker.hostname + ":" + std::to_string(worker.port) + ":" +
               std::to_string(worker.dataPort);
    // append , only if not last
    if (counter < noOfPartitions) {
      workers += ",";
    }
  }

  semantic_beam_search_logger_executor.info("semantic beam search" + workers);
  length = htonl(workers.size());
  semantic_beam_search_logger_executor.debug("Sending workers length: " +
                                             std::to_string(ntohl(length)));
  send(sockfd, &length, sizeof(length), 0);
  semantic_beam_search_logger_executor.debug("Sending workers: " + workers);
  if (send(sockfd, workers.c_str(), workers.size(), 0) <= 0) {
    semantic_beam_search_logger_executor.error("Failed to send query");
    close(sockfd);
    return;
  }
  bzero(ack, ACK_MESSAGE_SIZE);
  semantic_beam_search_logger_executor.debug("Waiting for ACK after query");
  if (recv(sockfd, ack, strlen("stream-c-length-ack"), 0) <= 0) {
    semantic_beam_search_logger_executor.info(ack);
    semantic_beam_search_logger_executor.error(
        "Failed to receive ACK after query");
    close(sockfd);
    return;
  }

  semantic_beam_search_logger_executor.info("Received ACK after query ");


  char start[ACK_MESSAGE_SIZE] = {0};
  recv(sockfd, &start, sizeof(start), 0);
  std::string start_msg(start);
  char ack2[ACK_MESSAGE_SIZE] = {0};
  recv(sockfd, &ack2, sizeof(start), 0);
  std::string ack2_msg(ack2);
  semantic_beam_search_logger_executor.info(start_msg);
  semantic_beam_search_logger_executor.info(
      "Semantic Beam Search request sent successfully");
  while (true) {
    char start[ACK_MESSAGE_SIZE] = {0};
    recv(sockfd, &start, sizeof(start), 0);
    std::string start_msg(start);
    if (JasmineGraphInstanceProtocol::QUERY_DATA_START != start_msg) {
      semantic_beam_search_logger_executor.error(
          "Error while receiving start command: " + start_msg);
      break;
    }
    send(sockfd, JasmineGraphInstanceProtocol::QUERY_DATA_ACK.c_str(),
         JasmineGraphInstanceProtocol::QUERY_DATA_ACK.length(), 0);

    int content_length;
    ssize_t return_status = recv(sockfd, &content_length, sizeof(int), 0);
    if (return_status > 0) {
      content_length = ntohl(content_length);
      semantic_beam_search_logger_executor.debug(
          "Received int =" + std::to_string(content_length));
    } else {
      semantic_beam_search_logger_executor.error(
          "Error while receiving content length");
      return;
    }
    send(sockfd,
         JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK.c_str(),
         JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK.length(), 0);

    std::string data(content_length, 0);
    size_t received = 0;
    while (received < content_length) {
      ssize_t ret = recv(sockfd, &data[received], content_length - received, 0);
      if (ret <= 0) {
        semantic_beam_search_logger_executor.error(
            "Error receiving request string");
        break;
      }
      received += ret;
    }
    send(sockfd, JasmineGraphInstanceProtocol::GRAPH_DATA_SUCCESS.c_str(),
         JasmineGraphInstanceProtocol::GRAPH_DATA_SUCCESS.length(), 0);


    semantic_beam_search_logger_executor.info("patition " +
                                              std::to_string(partitionId) +
                                              "Received graph data: " + data);
    if (data == "-1") {
      sharedBuffer.add(data);
      break;
    }
    sharedBuffer.add(data);
  }

  close(sockfd);
  semantic_beam_search_logger_executor.debug("Socket closed");
  return;
}

int SemanticBeamSearchExecutor::getUid() {
  static std::atomic<std::uint32_t> uid{0};
  return ++uid;
}
