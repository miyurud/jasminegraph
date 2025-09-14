"""Copyright 2025 JasmineGraph Team
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import json
import uuid

# Load input NBA KG
with open("data/input/terrorist_attack_simplekg.json", "r", encoding="utf-8") as infile:
    nba_kg = json.load(infile)

entities = nba_kg["entities"]
relations = nba_kg.get("relations", [])

# Mapping from entity ID to numeric ID and content
id_map = {}
id_counter = 0


def get_numeric_id(eid):
    """Get a numeric string ID for an entity."""
    global id_counter
    if eid not in id_map:
        id_map[eid] = str(id_counter)
        id_counter += 1
    return id_map[eid]


def filter_string_props(props):
    """Filter and convert properties to string if they are string-typed."""
    return {k: str(v) for k, v in props.items() if isinstance(v, str)}


# Write converted graph with properties
with open("data/output/terrorist_attack_simplekg1.txt", "w", encoding="utf-8") as outfile:
    for rel in relations:
        src_id = get_numeric_id(rel["subj_id"])
        dst_id = get_numeric_id(rel["obj_id"])

        src_entity = next((e for e in entities if e["eid"] == rel["subj_id"]), None)
        dst_entity = next((e for e in entities if e["eid"] == rel["obj_id"]), None)

        if not src_entity or not dst_entity:
            continue

        src_data = {
            "id": src_id,
            "label": src_entity["label"],
            "name": src_entity["name"],
        }
        src_data.update(filter_string_props(src_entity.get("properties", {})))

        dst_data = {
            "id": dst_id,
            "label": dst_entity["label"],
            "name": dst_entity["name"],
        }
        dst_data.update(filter_string_props(dst_entity.get("properties", {})))

        edge_properties = {
            "id": str(uuid.uuid4().int)[:8],
            "type": rel["label"],
            "description": f'{src_entity["name"]} -> {rel["label"]} -> {dst_entity["name"]}',
        }
        edge_properties.update(filter_string_props(rel.get("properties", {})))

        line = {
            "source": {"id": src_id, "properties": src_data},
            "destination": {"id": dst_id, "properties": dst_data},
            "properties": edge_properties,
        }

        outfile.write(json.dumps(line, ensure_ascii=False) + "\n")
