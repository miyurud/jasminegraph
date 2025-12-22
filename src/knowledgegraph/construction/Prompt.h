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
You MUST extract **ALL** named entities and construct an RDF (Resource Description Framework) graph from the text.
Do **NOT** skip any triples unless the subject or object is an ambiguous pronoun
(he, she, it, they, them, this, that).

Output format:
[
  [subject, predicate, object, subject_type, object_type],
  ...
]

STRICT RULES:
- Continue until you have processed the **entire chunk** fully.
- Do not stop early.
- Extract relations aligned with schema.org where possible.”
- Output ONLY valid JSON.

Now extract ALL triples from the text:
)";
}