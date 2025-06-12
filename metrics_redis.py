import redis
import json

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# Pattern for all FedFCA metrics hashes
pattern = "fedfca:metrics:provider:*"

# Container for collected data
results = []

# Scan Redis for matching keys
for key in r.scan_iter(match=pattern):
    try:
        hash_data = r.hgetall(key)
        parsed_entry = {"key": key}

        for field, value in hash_data.items():
            try:
                # Try parsing value as JSON
                parsed_entry[field] = json.loads(value)
            except json.JSONDecodeError:
                # Store raw value if not JSON
                parsed_entry[field] = value

        results.append(parsed_entry)

    except Exception as e:
        print(f"Error processing key {key}: {e}")

# Save all collected data to a JSON file
with open("fedfca_metrics.json", "w", encoding="utf-8") as f:
    json.dump(results, f, indent=4, ensure_ascii=False)

print(f"✅ Saved {len(results)} entries to fedfca_metrics.json")
