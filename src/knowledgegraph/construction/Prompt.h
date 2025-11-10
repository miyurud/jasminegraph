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
#include <string>

namespace Prompts {
inline const std::string KNOWLEDGE_EXTRACTION = R"(
-  Extract subgraphs with many meaningful, non-duplicate triples (facts) as possible
- Omit triples where subject/object is an ambiguous pronoun (he, she, it, they).
- Return only a JSON array of arrays in the form:
    [subject, predicate, object, subject_type, object_type].
)";
}
