//
// Created by sajeenthiran on 2025-08-11.
//

#ifndef JASMINEGRAPH_HDFSPIPELINE_H
#define JASMINEGRAPH_HDFSPIPELINE_H
#include <atomic>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <hdfs.h>
#include <cppkafka/cppkafka.h>

class SQLiteDBInterface;

class Pipeline {
public:
    Pipeline(hdfsFS fileSystem, const std::string &filePath, int numberOfPartitions, int graphId,

              std::string masterIP);
    void init();
    void startStreamingFromBufferToPartitions();

private:

    void streamFromHDFSIntoBuffer();
    void streamChunckToWorker(const std::string& chunk, int partitionId);
    void startStreamingFromBufferToWorkers();


    hdfsFS fileSystem;

    std::string filePath;
    std::queue<std::string> dataBuffer;

    std::mutex dataBufferMutex;
    std::condition_variable dataBufferCV;

    std::string masterIP;
    SQLiteDBInterface *sqlite;

    bool isReading;
    bool isProcessing;
    bool isDirected;
    bool isEdgeListType;
    int graphId;
    int numberOfPartitions;
    std::mutex dbLock;
};


// ---------- Thread-safe queue ----------
template<typename T>
class TSQueue {
public:
    void push(const T &v);
    bool pop(T &out);
    void close();
private:
    std::queue<T> q;
    std::mutex m;
    std::condition_variable cv;
    bool closed = false;
};

// ---------- Simple data structures ----------
struct Chunk {
    std::string doc_id;
    std::string text;
};

struct Triple {
    std::string src;
    std::string rel;
    std::string dst;
    std::string provenance; // optional metadata
};

// ---------- Kafka Producer wrapper ----------
class KafkaProducer {
    cppkafka::Producer producer;
    std::string topic;
public:
    KafkaProducer(const std::string &brokers, const std::string &topic_);
    void send(const std::string &message);
};

// ---------- Kafka Consumer wrapper ----------
class KafkaConsumer {
    cppkafka::Consumer consumer;
public:
    KafkaConsumer(const std::string &brokers, const std::string &groupId, const std::vector<std::string> &topics);
    std::string poll_msg(int timeout_ms = 1000);
};
// ---------- Utility: split document into chunks ----------
std::vector<Chunk> split_document(const std::string &doc_id, const std::string &text, size_t chunk_words = 50);

// ---------- Pipeline components ----------
void document_reader(const std::string &docs_dir, TSQueue<Chunk> &chunk_q);
void worker_pool(size_t worker_id, TSQueue<Chunk> &chunk_q, KafkaProducer &kprod, TSQueue<Triple> *local_out = nullptr);


#endif //JASMINEGRAPH_HDFSPIPELINE_H