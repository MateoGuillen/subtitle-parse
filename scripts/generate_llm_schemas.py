"""Generate LLM schemas for all 10 clustered titles via OpenRouter."""

import json
import os
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import OPENROUTER_API_KEY

BASE_URL = "https://openrouter.ai/api/v1/chat/completions"
MODELS = [
    "google/gemma-4-31b-it:free",
    "qwen/qwen3-coder:free",
    "nousresearch/hermes-3-llama-3.1-405b:free",
    "google/gemma-4-26b-a4b-it:free",
]
PROMPTS_PATH = Path("data/processed/section_clustering/llm_chat_prompts.json")
MASTER_PATH = Path("data/processed/section_clustering/master_schemas.json")
OUT_DIR = Path("data/external/llm_web_responses")
OUT_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "Authorization": f"Bearer {OPENROUTER_API_KEY}",
    "Content-Type": "application/json",
}


def call_llm(messages, model, max_tokens=4096, temperature=0.0):
    payload = {
        "model": model,
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": temperature,
    }
    for attempt in range(3):
        try:
            resp = requests.post(BASE_URL, headers=HEADERS, json=payload, timeout=300)
            if resp.status_code == 200:
                data = resp.json()
                choices = data.get("choices", [])
                if choices:
                    return choices[0].get("message", {}).get("content", "")
                print(f"  Response: {json.dumps(data)[:200]}")
                return ""
            elif resp.status_code == 429:
                err = resp.json().get("error", {}).get("message", "")
                print(f"  429: {err[:100]}")
                if attempt < 2:
                    wait = 5 * (2 ** attempt)
                    print(f"  Retrying in {wait}s...")
                    time.sleep(wait)
            else:
                print(f"  HTTP {resp.status_code}: {resp.text[:200]}")
                if attempt < 2:
                    time.sleep(2 ** attempt)
        except requests.exceptions.RequestException as e:
            print(f"  Request error: {e}")
            if attempt < 2:
                time.sleep(2 ** attempt)
    return ""


def parse_schema(response_text):
    json_str = response_text
    if "```json" in json_str:
        json_str = json_str.split("```json")[1].split("```")[0].strip()
    elif "```" in json_str:
        json_str = json_str.split("```")[1].split("```")[0].strip()
    try:
        return json.loads(json_str)
    except json.JSONDecodeError:
        return None


def main():
    if not OPENROUTER_API_KEY or OPENROUTER_API_KEY == "tu_clave_aqui":
        print("ERROR: OPENROUTER_API_KEY not configured in .env")
        return

    prompts_data = json.loads(PROMPTS_PATH.read_text(encoding="utf-8"))
    master = json.loads(MASTER_PATH.read_text(encoding="utf-8"))
    print(f"Loaded {len(prompts_data)} prompts, {len(master)} titles in master")

    for i, entry in enumerate(prompts_data):
        title = entry["title"]
        print(f"\n[{i+1}/{len(prompts_data)}] Processing '{title}'...")

        messages = [
            {"role": "system", "content": entry["prompt"]["system"]},
            {"role": "user", "content": entry["prompt"]["user"]},
        ]

        response_text = None
        used_model = None
        for model in MODELS:
            print(f"  Trying model: {model}")
            response_text = call_llm(messages, model)
            if response_text:
                used_model = model
                print(f"  OK with {model}")
                break
            print(f"  Failed with {model}")

        if not response_text:
            print(f"  [X] All models failed for '{title}'")
            continue

        safe_title = title.lower().replace(" ", "_").replace("/", "_")
        resp_path = OUT_DIR / f"openrouter_{safe_title}.txt"
        resp_path.write_text(response_text, encoding="utf-8")
        print(f"  Saved raw response ({len(response_text)} chars)")

        schema_data = parse_schema(response_text)
        if not schema_data or "schema" not in schema_data:
            print(f"  [X] Could not parse JSON schema from response")
            print(f"  First 300 chars: {response_text[:300]}")
            continue

        schema = schema_data["schema"]
        n_fields = len(schema)
        bin_count = sum(1 for f in schema.values() if f.get("type") == "binary")
        num_count = sum(1 for f in schema.values() if f.get("type") == "numeric")
        print(f"  Schema: {n_fields} fields ({bin_count} binary, {num_count} numeric)")

        if title not in master:
            print(f"  [X] Title '{title}' not in master_schemas.json!")
            continue

        master[title]["json_schema"] = schema
        master[title]["schema_justification"] = schema_data.get("justification", "")
        if schema_data.get("cluster_values"):
            master[title]["cluster_values"] = schema_data["cluster_values"]
        master[title]["llm_provider"] = "openrouter"
        master[title]["llm_model"] = used_model
        master[title]["schema_version"] = "v1"
        master[title]["imported_from"] = resp_path.name
        print(f"  [OK] Inserted into master_schemas.json")

    MASTER_PATH.write_text(json.dumps(master, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\n[DONE] Updated {MASTER_PATH}")


if __name__ == "__main__":
    main()
