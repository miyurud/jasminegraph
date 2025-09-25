import time
import requests

VLLM_URL = "http://10.10.14.108:6578/v1/completions"
MODEL = "meta-llama/Llama-3.2-3B-Instruct"  # Adjust as needed

prompt = """You are an expert information extractor specialized in knowledge graph construction. 
Extract all subject-predicate-object triples from the following text. 

Output each triple as a **JSON object**, separated by `#`. Use this format:

{
  "source": {
    "id": "<unique_node_id>",
    "properties": {
      "id": "<unique_node_id>",
      "label": "<EntityType>",
      "name": "<EntityName>"
    }
  },
  "destination": {
    "id": "<unique_node_id>",
    "properties": {
      "id": "<unique_node_id>",
      "label": "<EntityType>",
      "name": "<EntityName>"
    }
  },
  "properties": {
    "id": "<unique_relationship_id>",
    "type": "<Predicate>",
    "description": "<Human-readable description of the triple>"
  }
}

Instructions:
- Output **only JSON objects** separated by `#`.
- Use **consistent and unique IDs** by concatenating label + name in lowercase with underscores.
- Populate all fields accurately, including labels and descriptions.
- Extract **as many meaningful triples as possible**.

Example:

Text: 'Barack Obama was born in Honolulu on August 4, 1961. He served as the 44th President of the United States.'

JSON objects:

{
  "source": {
    "id": "person_barack_obama",
    "properties": {
      "id": "person_barack_obama",
      "label": "Person",
      "name": "Barack Obama"
    }
  },
  "destination": {
    "id": "location_honolulu",
    "properties": {
      "id": "location_honolulu",
      "label": "Location",
      "name": "Honolulu"
    }
  },
  "properties": {
    "id": "relationship_barack_obama_born_in_honolulu",
    "type": "born_in",
    "description": "Barack Obama was born in Honolulu on August 4, 1961"
  }
}
#
{
  "source": {
    "id": "person_barack_obama",
    "properties": {
      "id": "person_barack_obama",
      "label": "Person",
      "name": "Barack Obama"
    }
  },
  "destination": {
    "id": "organization_united_states",
    "properties": {
      "id": "organization_united_states",
      "label": "Organization",
      "name": "United States"
    }
  },
  "properties": {
    "id": "relationship_barack_obama_president_of_united_states",
    "type": "president_of",
    "description": "Barack Obama served as the 44th President of the United States"
  }
}

Now process the following text:

Radio City is India's first private FM radio station and was started on 3 July 2001.
It broadcasts on 91.1 (earlier 91.0 in most cities) megahertz from Mumbai (where it was started in 2004), Bengaluru (started first in 2001), Lucknow and New Delhi (since 2003).
It plays Hindi, English and regional songs.
It was launched in Hyderabad in March 2006, in Chennai on 7 July 2006 and in Visakhapatnam October 2007.
Radio City recently forayed into New Media in May 2008 with the launch of a music portal - PlanetRadiocity.com that offers music related news, videos, songs, and other music-related features.
The Radio station currently plays a mix of Hindi and Regional music.
Abraham Thomas is the CEO of the company.
Football in Albania existed before the Albanian Football Federation (FSHF) was created.
This was evidenced by the team's registration at the Balkan Cup tournament during 1929-1931, which started in 1929.
Albanian National Team was founded on June 6, 1930, but Albania had to wait 16 years to play its first international match and then defeated Yugoslavia in 1946.
In 1932, Albania joined FIFA (during the 12–16 June convention ) and in 1954 it was one of the founding members of UEFA.
Echosmith is an American corporate indie pop band formed in February 2009 in Chino, California.
Originally formed as a quartet of siblings, the band currently consists of Sydney, Noah and Graham Sierota, following the departure of eldest sibling Jamie in late 2016.
Echosmith started first as "Ready Set Go!" until they signed to Warner Bros. Records in May 2012.
They are best known for their hit song "Cool Kids", which reached number 13 on the Billboard Hot 100 and was certified double platinum by the RIAA in the United States and by ARIA in Australia.
\n\nJSON objects:\n
"""

start = time.time()
with requests.post(VLLM_URL, json={
    "model": MODEL,
    "prompt": prompt,
    "max_tokens": 3000,
    "stream": True
}, stream=True) as resp:
    if resp.status_code != 200:
        raise RuntimeError(f"vLLM request failed: {resp.status_code} {resp.text}")

    for line in resp.iter_lines(decode_unicode=True):
        if line.strip():
            print(line)
end = time.time()

# out = resp.json()
# tokens = len(out)
# tps = tokens / (end - start)

# print(out)
print(f"vLLM: tokens in {end - start:.2f}s →  tokens/sec")
