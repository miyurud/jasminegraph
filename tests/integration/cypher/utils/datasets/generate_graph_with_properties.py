import json
import random
import string
import os
target_size_gb = 0.0001
# File settings
output_file = "/tests/integration/env_init/data/graph_data_0.0001GB.txt"

target_size_bytes = target_size_gb * 1024**3

# Sample data pools
person_names = ["Alice", "Bob", "Charlie", "David", "Eva", "Fiona", "George", "Hannah", "Ian", "Julia"]
occupations = ["Engineer", "Doctor", "Artist", "Scientist", "Teacher", "Chef", "Lawyer", "Banker"]
location_names = ["City Library", "Central Park", "Town Bank", "Skyport Airport", "Tech Solutions Inc.", "Greenfield School"]
categories = ["Library", "Park", "Bank", "Airport", "Office", "School", "Restaurant", "Hospital", "Studio"]

relationship_types = ["FRIENDS", "NEIGHBORS", "WORKS_AT", "VISITS", "MANAGES"]

def random_id():
    return str(random.randint(1000, 9999))

def random_person():
    return {
        "id": random_id(),
        "label": "Person",
        "name": random.choice(person_names),
        "occupation": random.choice(occupations),
        "age": str(random.randint(20, 65))
    }

def random_location():
    return {
        "id": random_id(),
        "label": "Location",
        "name": random.choice(location_names),
        "category": random.choice(categories)
    }

def random_entity():
    return random_person() if random.random() < 0.5 else random_location()

def random_relationship(id_num, src, dst):
    rel_type = random.choice(relationship_types)
    src_name = src.get("name", "Unknown")
    dst_name = dst.get("name", "Unknown")
    desc = f"{src_name} {rel_type.lower()} {dst_name}."
    return {
        "id": str(id_num),
        "type": rel_type,
        "description": desc
    }

# Generate data
with open(output_file, "w") as f:
    total_bytes = 0
    entry_id = 0

    while total_bytes < target_size_bytes:
        src = random_entity()
        dst = random_entity()
        rel = random_relationship(entry_id, src, dst)

        entry = {
            "source": {"id": src["id"], "properties": src},
            "destination": {"id": dst["id"], "properties": dst},
            "properties": rel
        }

        line = json.dumps(entry) + "\n"
        f.write(line)
        total_bytes += len(line)
        entry_id += 1

        if entry_id % 100000 == 0:
            print(f"{entry_id} entries written, ~{total_bytes / 1024**3:.2f} GB")

print(f"✅ Finished writing {entry_id} entries to {output_file}")
