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
You MUST extract **ALL** named entities and construct an RDF (Resource Description Framework) sub graph from the text.
Do **NOT** skip any triples unless the subject or object is an ambiguous pronoun
(he, she, it, they, them, this, that).

Output format:
[
  [subject, predicate, object, subjectType, objectType],
  ...
]

STRICT RULES:
- Continue until the entire chunk is fully processed
- Make each triple interpretable in isolation
- Someone reading a single triple should understand who did what, where, and in what context
- Do NOT skip any triples unless subject or object is an ambiguous pronoun
- Predicates MUST be from schema.org (canonical form only)
 -Every triple MUST contain exactly five fields: subject, predicate, object, subject_type, object_type.
 - Every field must be a string
- Output ONLY valid JSON
- NEVER leave any field empty, null, or omitted

TYPING RULES (MANDATORY):
- EVERY triple MUST include subject_type and object_type
- Types MUST be schema.org types
- Use the MOST SPECIFIC type possible
- If unsure, fall back to a superclass
- If still unsure, use "Thing"

WORKED EXAMPLE (FOLLOW EXACTLY):

Input text:
Apple Inc. was founded by Steve Jobs and Steve Wozniak in Cupertino.
Tim Cook is the current CEO of Apple Inc.

Correct output:
[
  ["Apple Inc.", "founder", "Steve Jobs", "Organization", "Person"],
  ["Apple Inc.", "founder", "Steve Wozniak", "Organization", "Person"],
  ["Apple Inc.", "foundingLocation", "Cupertino", "Organization", "Place"],
  ["Tim Cook", "jobTitle", "CEO", "Person", "DefinedTerm"],
  ["Tim Cook", "worksFor", "Apple Inc.", "Person", "Organization"]
]

)";
}