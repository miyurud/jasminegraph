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
