import requests
import time
import json

# ---------------------------------------------------
# Configuration
# ---------------------------------------------------
# model_name = "llama3"  # Change to your model
model_name = "gemma3:12b"  # Change to your model


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

The Ellerbusch Site ( 12-W-56 ) is a small but significant archaeological site in the southwestern part of the U.S. state of Indiana .\nUnlike many sites created by people of the same culture , it occupies an upland site near a major river floodplain .\nIts existence appears to have been the result of the coincidence of periods of peace and growth in the related Angel Site , which led some townspeople to leave their homes for new villages that were more convenient for resource gathering .\nResearched partly because of its small size , Ellerbusch has produced information that greatly increases present awareness of other small sites and of its culture 's overall patterns of settlement in the region 
"""

# Ollama endpoints
OLLAMA_URL = "http://10.10.21.26:11440/api/generate"

# ---------------------------------------------------
# Helper function to run inference
# ---------------------------------------------------
def run_inference(use_gpu=True):
    """
    Run inference on Ollama with CPU or GPU.
    use_gpu=True -> GPU
    use_gpu=False -> CPU
    """
    headers = {"Content-Type": "application/json"}
    payload = {
        "model": model_name,
        "prompt": prompt,
        "options": {
            "num_gpu": 1 if use_gpu else 0,  # force CPU or GPU
        },
        "stream": False  # return full output at once
    }

    start_time = time.time()
    response = requests.post(OLLAMA_URL, headers=headers, data=json.dumps(payload))
    end_time = time.time()

    if response.status_code != 200:
        raise RuntimeError(f"Error: {response.text}")

    output = response.json()["response"]
    latency = end_time - start_time
    return output, latency

# ---------------------------------------------------
# Benchmark GPU
# ---------------------------------------------------
print("\nRunning on GPU...")
run_inference(use_gpu=True)
# print(f"GPU Latency: {gpu_latency:.2f} seconds")
print("\nRunning on GPU...")
gpu_output, gpu_latency = run_inference(use_gpu=True)
print(f"GPU Latency: {gpu_latency:.2f} seconds")
# -----------------------------
# ---------------------------------------------------
# Benchmark CPU
# ---------------------------------------------------
print("Running on CPU...")
run_inference(use_gpu=False)
cpu_output, cpu_latency = run_inference(use_gpu=False)
print(f"CPU Latency: {cpu_latency:.2f} seconds")


# Results
# ---------------------------------------------------
print("\n--- Comparison ---")
# print(f"CPU: {cpu_latency:.2f} s")
print(f"GPU: {gpu_latency:.2f} s")
print("\nSample Output (GPU):", gpu_output, "...")
