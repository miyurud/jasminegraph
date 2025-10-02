import json

# Replace with your dataset file
with open("/home/sajeenthiran/FYP/Data/QA/hotpot_train_v1.1.json", "r", encoding="utf-8") as f:
    data = json.load(f)


print(data[:3])
print(f"Total records: {len(data)}")
# # Extract questions and answers into dictionaries
# qa_pairs = [
#     {"id": item["_id"], "question": item["question"], "answer": item["answer"]}
#     for item in data if "question" in item and "answer" in item
# ]
#
# # Print sample output to console
# for qa in qa_pairs[:5]:  # just show first 5 to avoid flooding
#     print(json.dumps(qa, indent=2))
#     print("-" * 60)

# # Save as JSON
# with open("qa_pairs.json", "w", encoding="utf-8") as f:
#     json.dump(qa_pairs, f, indent=2, ensure_ascii=False)
#
# print(f"✅ Saved {len(qa_pairs)} QA pairs to qa_pairs.json")
#


import json
import os

# Input dataset file
input_file = "/home/sajeenthiran/FYP/Data/QA/hotpot_train_v1.1.json"
output_dir = "hotpot_qa_records"

# Load dataset
with open(input_file, "r", encoding="utf-8") as f:
    data = json.load(f)

print(f"Total records: {len(data)}")

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

# Process each record
for item in data[:20]:
    record_id = item["_id"]
    record_folder = os.path.join(output_dir, record_id)
    os.makedirs(record_folder, exist_ok=True)

    # Save context as a text file
    context_path = os.path.join(record_folder, "text.txt")
    with open(context_path, "w", encoding="utf-8") as f:
        for title, sentences in item["context"]:
            f.write(f"### {title}\n")
            for sent in sentences:
                f.write(sent.strip() + "\n")
            f.write("\n")

    # Save QA pair as JSON
    qa_data = {
        "id": record_id,
        "question": item.get("question", ""),
        "answer": item.get("answer", "")
    }
    qa_path = os.path.join(record_folder, "qa_pairs.json")
    with open(qa_path, "w", encoding="utf-8") as f:
        json.dump(qa_data, f, indent=2, ensure_ascii=False)

print(f"✅ Finished writing {len(data)} records into {output_dir}")
