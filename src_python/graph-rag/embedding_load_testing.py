import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# ----------------------------
# CONFIGURATION
# ----------------------------
URL = "http://10.10.21.26:11434/v1/embeddings"
MODEL = "nomic-embed-text"

# Example texts (can expand for more edges)
texts = [
    "Abraham Thomas is the CEO of the company",
    "Who is the CEO of Radio City?",
    "The quick brown fox jumps over the lazy dog",
    "Artificial intelligence is transforming industries"
]

NUM_USERS = 50  # Simulated concurrent users
NUM_REQUESTS_PER_USER = 10  # Number of requests per user

# ----------------------------
# FUNCTION TO CALL EMBEDDINGS
# ----------------------------
def call_embedding(text_batch):
    payload = {
        "model": MODEL,
        "input": text_batch
    }
    try:
        response = requests.post(URL, headers={"Content-Type": "application/json"}, data=json.dumps(payload))
        response.raise_for_status()
        return response.json()
    except Exception as e:
        return {"error": str(e)}

# ----------------------------
# LOAD TEST WITH THREADPOOL
# ----------------------------
def main():
    all_results = []
    with ThreadPoolExecutor(max_workers=NUM_USERS) as executor:
        futures = []
        for _ in range(NUM_USERS):
            for _ in range(NUM_REQUESTS_PER_USER):
                # Randomly pick texts for this request
                futures.append(executor.submit(call_embedding, texts))

        for future in as_completed(futures):
            result = future.result()
            all_results.append(result)

    print(f"Completed {len(all_results)} requests")
    print("Sample response:", all_results[0])

if __name__ == "__main__":
    main()
