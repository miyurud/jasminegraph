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
You MUST extract **ALL** named entities and construct an RDF (Resource Description Framework) subgraph from this chunk of a larger text corpus.
Do **NOT** skip any triples unless the subject or object is an ambiguous pronoun
(he, she, it, they, them, this, that).


Output format WITH relation metadata (ONLY when temporal and/or spatial information is explicitly present):
[
   [subject, predicate, object, subjectType, objectType, when, where],
  ...
]

STRICT RULES:
- Extract **every possible factual relation** in the text, even if many.
- Continue until you have processed the **entire chunk** fully.
- Do not stop early.
- Predicates MUST be valid schema.org properties (canonical form only)
- Prefer the MOST GENERAL valid schema.org superclass for entity types
  (e.g., Person instead of Actor, Place instead of City)
- Every tuple MUST contain 5 or more fields
- Every field MUST be a non-empty string
- NEVER use pronouns (He, She, It, They, etc.) as subject or object
- Output ONLY valid JSON

WORKED EXAMPLE (FOLLOW EXACTLY):

Input text:
Apple Inc. was founded by Steve Jobs and Steve Wozniak in Cupertino.
Tim Cook is the current CEO of Apple Inc.
Annette Bening played Lady Macbeth in 1984 at the American Conservatory Theatre.

Correct output:
[
  ["Apple Inc.", "founder", "Steve Jobs", "Organization", "Person"],
  ["Apple Inc.", "founder", "Steve Wozniak", "Organization", "Person"],
  ["Apple Inc.", "foundingLocation", "Cupertino", "Organization", "Place"],
  ["Tim Cook", "jobTitle", "CEO", "Person", "DefinedTerm"],
  ["Tim Cook", "worksFor", "Apple Inc.", "Person", "Organization"],
["Annette Bening", "actor", "Lady Macbeth", "Person", "FictionalCharacter", "1984", "American Conservatory Theatre"],
  ["Annette Bening", "performerIn", "American Conservatory Theatre", "Person", "Organization", "1984", "San Francisco"]

)";
}