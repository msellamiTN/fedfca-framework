import os
import json
import csv

# Define the folder containing the JSON files
folder_path = r"D:\Research2023\FFCA\FedFCA\fedfac-framework.v4\expriments_core_fedfca"

# Loop through all files in the folder
for filename in os.listdir(folder_path):
    if filename.endswith('.json'):
        print(f"Processing {filename}")
        json_path = os.path.join(folder_path, filename)
        csv_filename = os.path.splitext(filename)[0] + '.csv'
        csv_path = os.path.join(folder_path, csv_filename)

        try:
            # Load the JSON file
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Make sure it's a list of dictionaries
            if isinstance(data, list) and all(isinstance(d, dict) for d in data):
                # Collect all unique keys as column headers
                fieldnames = sorted({key for item in data for key in item.keys()})

                # Write the CSV file
                with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(data)

                print(f"[✅] Converted: {filename} → {csv_filename}")
            else:
                print(f"[⚠️] Skipped (not a list of dictionaries): {filename}")

        except Exception as e:
            print(f"[❌] Error processing {filename}: {e}")
