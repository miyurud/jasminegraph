import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed


VLLM_URL = "http://10.10.21.26:6578/v1/completions"
MODEL = "RedHatAI/Meta-Llama-3.1-8B-Instruct-FP8"  # Adjust as needed

def run_request(prompt, max_tokens=3000):
    start = time.time()
    outputs = []

    with requests.post(
        VLLM_URL,
        json={"model": MODEL, "prompt": prompt, "max_tokens": max_tokens, "stream": True},
        stream=True,
    ) as resp:
        if resp.status_code != 200:
            raise RuntimeError(f"vLLM request failed: {resp.status_code} {resp.text}")

        for line in resp.iter_lines(decode_unicode=True):
            if line and line.strip():
                outputs.append(line)

    end = time.time()
    return {
        "outputs": outputs,
        "elapsed": end - start,
        "tokens": sum(len(o) for o in outputs),  # crude measure
    }

if __name__ == "__main__":
    prompt ="""You are an expert information extractor specialized in knowledge graph construction. 
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

Following is the text:

Radio City is India's first private FM radio station and was started on 3 July 2001.
It broadcasts on 91.1 (earlier 91.0 in most cities) megahertz from Mumbai (where it was started in 2004), Bengaluru (started first in 2001), Lucknow and New Delhi (since 2003).
It plays Hindi, English and regional songs.
It was launched in Hyderabad in March 2006, in Chennai on 7 July 2006 and in Visakhapatnam October 2007.
Radio City recently forayed into New Media in May 2008 with the launch of a music portal - PlanetRadiocity.com that offers music related news, videos, songs, and other music-related features.
The Radio station currently plays a mix of Hindi and Regional music.
Abraham Thomas is the CEO of the company.

\n\nJSON objects:\n
"""
    prompts = [prompt] * 4  # Example: run 3 concurrent requests

    start_all = time.time()
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(run_request, p) for p in prompts]

        for i, f in enumerate(as_completed(futures), 1):
            result = f.result()
            # print(f"\n--- Response {i} ---")
            # for chunk in result["outputs"]:
            #     # print(chunk)
            # print(f"\nElapsed: {result['elapsed']:.2f}s, Tokens≈{result['tokens']}")

    print(f"\nTotal elapsed (all concurrent): {time.time() - start_all:.2f}s")
