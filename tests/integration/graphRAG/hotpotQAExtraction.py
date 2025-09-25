import json

# Replace with your dataset file
with open("/home/sajeenthiranp/fork/jasminegraph/hotpot_train_v1.1.json", "r", encoding="utf-8") as f:
    data = json.load(f)

# Extract questions and answers into dictionaries
qa_pairs = [
    {"id": item["_id"], "question": item["question"], "answer": item["answer"]}
    for item in data if "question" in item and "answer" in item
]

# Print sample output to console
for qa in qa_pairs[:5]:  # just show first 5 to avoid flooding
    print(json.dumps(qa, indent=2))
    print("-" * 60)

# Save as JSON
with open("qa_pairs.json", "w", encoding="utf-8") as f:
    json.dump(qa_pairs, f, indent=2, ensure_ascii=False)

print(f"✅ Saved {len(qa_pairs)} QA pairs to qa_pairs.json")
