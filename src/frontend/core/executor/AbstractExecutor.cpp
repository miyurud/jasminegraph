/**
Copyright 2021 JasmineGraph Team
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

#include "AbstractExecutor.h"

AbstractExecutor::AbstractExecutor(JobRequest jobRequest) { this->request = jobRequest; }

AbstractExecutor::AbstractExecutor() {}

std::vector<std::vector<string>> AbstractExecutor::getCombinations(std::vector<string> inputVector) {
    std::vector<std::vector<string>> combinationsList;
    std::vector<std::vector<int>> combinations;

    // Below algorithm will get all the combinations of 3 workers for given set of workers
    std::string bitmask(3, 1);
    bitmask.resize(inputVector.size(), 0);

    do {
        std::vector<int> combination;
        for (int i = 0; i < inputVector.size(); ++i) {
            if (bitmask[i]) {
                combination.push_back(i);
            }
        }
        combinations.push_back(combination);
    } while (std::prev_permutation(bitmask.begin(), bitmask.end()));

    for (std::vector<std::vector<int>>::iterator combinationsIterator = combinations.begin();
         combinationsIterator != combinations.end(); ++combinationsIterator) {
        std::vector<int> combination = *combinationsIterator;
        std::vector<string> tempWorkerIdCombination;

        for (std::vector<int>::iterator combinationIterator = combination.begin();
             combinationIterator != combination.end(); ++combinationIterator) {
            int index = *combinationIterator;

            tempWorkerIdCombination.push_back(inputVector.at(index));
        }

        combinationsList.push_back(tempWorkerIdCombination);
    }

    return combinationsList;
}
