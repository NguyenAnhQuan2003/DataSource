import json
from pathlib import Path

def convert_json_to_line(json_file):
    json_file = Path(json_file)
    jsonl_file = json_file.with_suffix(".jsonl")

    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    with open(jsonl_file, "w", encoding="utf-8") as f:
        for doc in data:
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")

    print(f"Đã convert {json_file} -> {jsonl_file}")
    return jsonl_file