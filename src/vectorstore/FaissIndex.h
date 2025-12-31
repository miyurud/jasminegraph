/**
Copyright 2025 JasmineGraph Team
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
#pragma once
#include <faiss/IndexFlat.h>
#include <faiss/IndexIDMap.h>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
class FaissIndex {
 public:
  static FaissIndex* getInstance(int embeddingDim, const std::string& filepath);

  ~FaissIndex();
  faiss::idx_t add(const std::vector<float>& embedding, std::string nodeId);

  faiss::idx_t add(const std::vector<float>& embedding);

  // Search returns (id, distance)
  std::vector<std::pair<faiss::idx_t, float>> search(
      const std::vector<float>& query, int k);

  void save(const std::string& filepath);
  void save();
  void load(const std::string& filepath);
  std::vector<float> getEmbeddingById(std::string id);
  std::string getNodeIdFromEmbeddingId(faiss::idx_t embeddingId);
  std::string getNodeIdFromEmbeddingId(std::string embeddingId) const;

 private:
  FaissIndex(int embeddingDim, const std::string& filepath);

  int dim;
  faiss::IndexFlatL2* index;
  std::mutex mtx;
  std::string filePath;
  std::unordered_map<std::string, faiss::idx_t>
      nodeIdToEmbeddingIdMap;  // Maps node IDs to FAISS IDs
  std::unordered_map<faiss::idx_t, std::string> embeddingIdToNodeIdMap;

  static std::unique_ptr<FaissIndex> instance;
  static std::once_flag initFlag;
};
