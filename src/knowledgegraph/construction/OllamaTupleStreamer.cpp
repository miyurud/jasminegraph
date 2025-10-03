#include "OllamaTupleStreamer.h"
#include <curl/curl.h>
#include <nlohmann/json.hpp>
#include <iostream>
#include <thread>   // for std::this_thread::sleep_for
#include <chrono>   // for std::chrono::seconds
#include "../../util/logger/Logger.h"

using json = nlohmann::json;
Logger ollama_tuple_streamer_logger;

OllamaTupleStreamer::OllamaTupleStreamer(const std::string& modelName,  const std::string& host)
    : model(modelName), host(host) {
    curl_global_init(CURL_GLOBAL_DEFAULT);
    // ollama_tuple_streamer_logger.init("OllamaTupleStreamer");
    ollama_tuple_streamer_logger.info("Initialized OllamaTupleStreamer with model: " + modelName + ", host: " + host);
}


size_t OllamaTupleStreamer::StreamCallback(char* ptr, size_t size, size_t nmemb, void* userdata) {
    size_t totalSize = size * nmemb;
    StreamContext* ctx = static_cast<StreamContext*>(userdata);

    std::string incoming(ptr, totalSize);
    size_t start = 0;
    while (true) {
        size_t pos = incoming.find("\n", start);
        if (pos == std::string::npos) {
            break;
        }

        std::string line = incoming.substr(start, pos - start); start = pos + 1;
        if (line.empty())
        {
            ollama_tuple_streamer_logger.info("Skipping empty line"); continue;
        }

        try {
            auto j = json::parse(line);

            // Completed tuple
            if (j.value("done", false)) {
                // std::string partial = j.value("response", "");
                ollama_tuple_streamer_logger.info("recieved done partial: "+ ctx->current_tuple );

                    // std::string tupleStr = ctx->current_tuple;
                    ctx->buffer->add("-1");
                    ctx->current_tuple.clear();

                break;

            }
            else if (j.contains("response")) {
                std::string partial = j["response"];
                size_t s = 0;
                ollama_tuple_streamer_logger.info("partial: "+ partial );


                size_t i = 0;
                // int braceDepth = 0;

                while (i < partial.size()) {
                    size_t bpos = partial.find_first_of("[]", i);
                    if (bpos == std::string::npos) {
                        if (ctx->braceDepth >= 2) {
                            ctx->current_tuple.append(partial, i, std::string::npos);
                        }
                        break;
                    }

                    if (ctx->braceDepth >= 2) {
                        ctx->current_tuple.append(partial, i, bpos - i);
                    }

                    char c = partial[bpos];

                    if (c == '[') {
                        ctx->braceDepth++;
                        if (ctx->braceDepth >= 2)
                        {
                            ctx->current_tuple.push_back(c);
                        }
                    } else { // '}'
                        ctx->braceDepth--;
                        ctx->current_tuple.push_back(c);

                     if (ctx->braceDepth == 1) {
                         ollama_tuple_streamer_logger.info("current: "+ctx->current_tuple);

    try {
        auto triple = json::parse(ctx->current_tuple);
        if (triple.is_array()) {

                if (triple.is_array() && triple.size() == 5) {
                    std::string subject = triple[0].get<std::string>();
                    std::string predicate = triple[1].get<std::string>();
                    std::string object = triple[2].get<std::string>();
                    std::string subject_type = triple[3].get<std::string>();
                    std::string object_type = triple[4].get<std::string>();

                    json formattedTriple = {
                        {"source", {
                {"id", subject},
                {"properties", {
                    {"id", subject},
                    {"label", subject_type},
                    {"name", subject}
                }}
                        }},
                        {"destination", {
                {"id", object},
                {"properties", {
                    {"id", object},
                    {"label", object_type},
                    {"name", object}
                }}
                        }},
                        {"properties", {
                {"id", subject + "_" + predicate + "_" + object},
                {"type", predicate},
                {"description", subject + " " + predicate + " " + object}
                        }}
                    };

                    // Add the formatted triple to the buffer
                    ctx->buffer->add(formattedTriple.dump());
                    ollama_tuple_streamer_logger.debug("✅ Added formatted triple: " + formattedTriple.dump());
                }
            }

    } catch (const std::exception& ex) {
        ollama_tuple_streamer_logger.error("❌ JSON array parse failed: " + std::string(ex.what()));
    }
    ctx->current_tuple.clear();
}

                    }
                    i = bpos + 1;
                }



            }
        } catch (...) {
            ollama_tuple_streamer_logger.info("99 Malformed/partial JSON ignored: " + line);
                ctx->buffer->add("-1");

        }
    }


    return totalSize;
}
void OllamaTupleStreamer::streamChunk(const std::string& chunkKey,
                                      const std::string& chunkText,
                                      SharedBuffer& tupleBuffer) {


             const int maxRetries = 10;                 // number of retries before giving up
    const int baseDelaySeconds = 50;           // wait time between retries (exponential backoff)

    int attempt = 0;
    CURLcode res;
    StreamContext ctx{chunkKey, &tupleBuffer,  "", true };

    do {
    CURL* curl = curl_easy_init();
    if (!curl) {
        std::cerr << "Failed to initialize CURL\n";
        return;
    }

        ollama_tuple_streamer_logger.debug("attempt: "+attempt);
        ollama_tuple_streamer_logger.debug("Chunk : "+ chunkText);
std::string url = host + "/api/generate";

    // Use 127.0.0.1 explicitly to avoid IPv6 localhost issues
    ollama_tuple_streamer_logger.info("Connecting to Ollama server at: " + host);
curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_POST, 1L);

    json j;
    j["model"] = model;
    j["max_tokens"] = 8000;
        // j["num_predict"] = 8000;

//   j["prompt"]  =
//               R"(You are an expert information extractor specialized in knowledge graph construction.
//         Your task is to extract all subject–predicate–object triples from the given text and output them in a strict JSON format.
//
//         Instructions:
//         - Output must consist ONLY of JSON objects separated by the symbol "#". No extra text.
//         - Each JSON object must have exactly three sections: "source", "destination", and "properties".
//         - Use consistent and unique IDs: concatenate entity type + entity name in lowercase with underscores (e.g., person_barack_obama).
//         - Use Wikidata-style entity labels for "label" fields (e.g., Person, Location, Organization, Event).
//         - Use Wikidata relation types for "type" (e.g., born_in, educated_at, member_of, spouse, president_of).
//         - "description" must be a faithful short sentence or phrase directly derived from the text.
//         - Omit extracting triples if the subject or object is an ambiguous pronoun (he, she, it, they) with no clear reference.
//         - Extract as many meaningful, non-duplicate triples as possible.
//         - Ensure that every triple is factual and derived explicitly from the text.
//
//         Use this format:
//         {
//           "source": {
//             "id": "<unique_node_id>",
//             "properties": {
//               "id": "<unique_node_id>",
//               "label": "<EntityType>",
//               "name": "<EntityName>"
//             }
//           },
//           "destination": {
//             "id": "<unique_node_id>",
//             "properties": {
//               "id": "<unique_node_id>",
//               "label": "<EntityType>",
//               "name": "<EntityName>"
//             }
//           },
//           "properties": {
//             "id": "<unique_relationship_id>",
//             "type": "<Predicate>",
//             "description": "<Human-readable description of the triple>"
//           }
//         }
//
//         Now process the following text:
//         )" + chunkText + R"(
//
//         JSON objects:
//         )";

//         j["prompt"]  =
//               R"(You are an expert information extractor specialized in knowledge graph construction.
//         Your task is to extract all subject–predicate–object triples from the given text and output them in a strict JSON format.
//
//         Instructions:
//         - Output must consist ONLY of JSON objects separated by the symbol "#". No extra text.
//         - Each JSON object must have exactly three sections: "source", "destination", and "properties".
//         - Use consistent and unique IDs: concatenate entity type + entity name in lowercase with underscores (e.g., person_barack_obama).
//         - Use Wikidata-style entity labels for "label" fields (e.g., Person, Location, Organization, Event).
//         - Use Wikidata relation types for "type" (e.g., born_in, educated_at, member_of, spouse, president_of).
//         - "description" must be a faithful short sentence or phrase directly derived from the text.
//         - Omit extracting triples if the subject or object is an ambiguous pronoun (he, she, it, they) with no clear reference.
//         - Extract as many meaningful, non-duplicate triples as possible.
//         - Ensure that every triple is factual and derived explicitly from the text.
//
//         Use this format:
//         {
//           "source": {
//             "id": "<unique_node_id>",
//             "properties": {
//               "id": "<unique_node_id>",
//               "label": "<EntityType>",
//               "name": "<EntityName>"
//             }
//           },
//           "destination": {
//             "id": "<unique_node_id>",
//             "properties": {
//               "id": "<unique_node_id>",
//               "label": "<EntityType>",
//               "name": "<EntityName>"
//             }
//           },
//           "properties": {
//             "id": "<unique_relationship_id>",
//             "type": "<Predicate>",
//             "description": "<Human-readable description of the triple>"
//           }
//         }
//
//         Now process the following text:
//         )" + chunkText + R"(
//
//         JSON objects:
//         )";

        j["prompt"]  =  R"(
- Extract as many meaningful, non-duplicate triples (facts) as possible. You should not miss any facts.
- Omit extracting triples only for the subject or object which have ambiguous pronoun (he, she, it, they) with no clear reference.
- Return only a JSON array of arrays in the form:
    [subject, predicate, object, subject_type, object_type].

Example:
    Text: Radio City is India's first private FM radio station and was started on 3 July 2001. It plays Hindi, English and regional songs. Radio City recently forayed into New Media in May 2008 with the launch of a music portal - PlanetRadiocity.com that offers music related news, videos, songs, and other music-related features.
    Array: [["Radio City", "located in", "India", "Organization", "Location"], ["Radio City", "is", "private FM radio station", "Organization", "Other"], ["Radio City", "started on", "3 July 2001", "Organization", "Date"], ["Radio City", "plays songs in", "Hindi", "Organization", "Other"], ["Radio City", "plays songs in", "English", "Organization", "Other"], ["Radio City", "forayed into", "New Media", "Organization", "Event"], ["Radio City", "launched", "PlanetRadiocity.com", "Organization", "Organization"], ["PlanetRadiocity.com", "launched in", "May 2008", "Organization", "Date"], ["PlanetRadiocity.com", "is", "music portal", "Organization", "Type"], ["PlanetRadiocity.com", "offers", "news", "Organization", "News"], ["PlanetRadiocity.com", "offers", "videos", "Organization", "Video"], ["PlanetRadiocity.com", "offers", "songs", "Organization", "Song"]]

Now process the following text:
        )" + chunkText + R"(

Array:
        )";
    j["stream"] = true;
    std::string postFields;
    try{
    postFields = j.dump();
    ollama_tuple_streamer_logger.debug("Post fields: " + postFields);
    }
    catch (const std::exception& e) {
        ollama_tuple_streamer_logger.error("JSON dump error: " + std::string(e.what()));
         ctx.buffer->add("-1");
        return ;
    }

    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, postFields.c_str());
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, postFields.size());

    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, StreamCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &ctx);


        // Enforce strong TLS
        curl_easy_setopt(curl, CURLOPT_SSLVERSION, CURL_SSLVERSION_TLSv1_2);

        // Verify certificate is trusted
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);

        // Verify hostname matches certificate
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);

        curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 50L); // fail fast if server not reachable
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, 12000L);       // max time for entire request
        curl_easy_setopt(curl, CURLOPT_LOW_SPEED_LIMIT, 1L);
        curl_easy_setopt(curl, CURLOPT_LOW_SPEED_TIME, 120L); // abort if <10 B/s for 30s

        // TCP keepalive to detect dead peers
        curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);
        curl_easy_setopt(curl, CURLOPT_TCP_KEEPIDLE, 60L);
        curl_easy_setopt(curl, CURLOPT_TCP_KEEPINTVL, 30L);
        res = curl_easy_perform(curl);


        // ctx.buffer->add("-1");
    if (res != CURLE_OK) {
        std::cerr << "Curl error: " << curl_easy_strerror(res) << "\n";
    }

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);

       if (res != CURLE_OK && attempt < maxRetries - 1) {
            int waitTime = baseDelaySeconds *  attempt;  // exponential backoff
            std::cerr << "Retrying in " << waitTime << " seconds...\n";
            std::this_thread::sleep_for(std::chrono::seconds(waitTime));
        }


        attempt++;
    } while (((res != CURLE_OK && attempt < maxRetries) || !ctx.isSuccess) );

    if (res != CURLE_OK) {
        std::cerr << "Failed after " << maxRetries << " attempts.\n";
        ctx.buffer->add("-1");


    }
        // ctx.buffer->add("-1");

}
