import json
import random
import os

# Settings
target_size_gb = 0.55
output_file = "/home/ubuntu/software/jasminegraph/tests/integration/env_init/data/graph_data_0.55GB.txt"
target_size_bytes = target_size_gb * 1024**3

NUM_NODES = 10000  # Adjust based on your size goal
EDGES_PER_NODE = 500  # Each node will have ~5 relationships

# Data pools
person_names = ["Alice", "Bob", "Charlie", "David", "Eva", "Fiona", "George", "Hannah", "Ian", "Julia"]
occupations = ["Engineer", "Doctor", "Artist", "Scientist", "Teacher", "Chef", "Lawyer", "Banker"]
location_names = ["City Library", "Central Park", "Town Bank", "Skyport Airport", "Tech Solutions Inc.", "Greenfield School"]
categories = ["Library", "Park", "Bank", "Airport", "Office", "School", "Restaurant", "Hospital", "Studio"]

relationship_types = ["FRIENDS", "NEIGHBORS", "WORKS_AT", "VISITS", "MANAGES"]

# Track used IDs
used_ids = set()

def generate_unique_id():
    while True:
        new_id = str(random.randint(0, NUM_NODES))
        if new_id not in used_ids:
            used_ids.add(new_id)
            return new_id

def create_person():
    return {
        "id": generate_unique_id(),
        "label": "Person",
        "name": random.choice(person_names),
        "occupation": random.choice(occupations),
        "age": str(random.randint(20, 65))
    }

def create_location():
    return {
        "id": generate_unique_id(),
        "label": "Location",
        "name": random.choice(location_names),
        "category": random.choice(categories)
    }

def create_relationship(rel_id, src, dst):
    rel_type = random.choice(relationship_types)
    desc = f"{src['name']} {rel_type.lower()} {dst['name']}."
    return {
        "id": str(rel_id),
        "type": rel_type,
        "description": desc
    }

# Step 1: Pre-generate nodes
nodes = []
for i in range(NUM_NODES):
    node = create_person() if random.random() < 0.5 else create_location()
    nodes.append(node)

# Step 2: Generate relationships
with open(output_file, "w") as f:
    total_bytes = 0
    rel_id = 0

    for src in nodes:
        for _ in range(EDGES_PER_NODE):
            dst = random.choice(nodes)
            while dst["id"] == src["id"]:
                dst = random.choice(nodes)  # Avoid self-loop
            rel = create_relationship(rel_id, src, dst)

            entry = {
                "source": {"id": src["id"], "properties": src},
                "destination": {"id": dst["id"], "properties": dst},
                "properties": rel
            }

            line = json.dumps(entry) + "\n"
            f.write(line)
            total_bytes += len(line)
            rel_id += 1

            if total_bytes >= target_size_bytes:
                print(f"Reached target size: {total_bytes / 1024**3:.2f} GB")
                break

        if total_bytes >= target_size_bytes:
            break

        if rel_id % 100000 == 0:
            print(f"{rel_id} relationships written, ~{total_bytes / 1024**3:.2f} GB")

print(f"✅ Finished writing {rel_id} relationships to {output_file}")
