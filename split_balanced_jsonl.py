import json
import os
import math

# Configuration
input_file = "reasoning-instruct-distill-mix/standardized_combined_dataset.json"
output_dir = "subsets_for_ps_jsonl/"
MAX_FILE_SIZE_MB = 95 
MAX_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024

os.makedirs(output_dir, exist_ok=True)

def split_balanced_jsonl(source_path, target_dir):
    print(f"Reading {source_path}...")
    
    try:
        with open(source_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if not isinstance(data, list):
            print("Error: JSON root must be a list.")
            return

        total_rows = len(data)
        # Estimate parts based on a safe average (adjust if your dataset is huge)
        # We start with 2 parts, but the loop will create more if size is exceeded.
        num_parts_estimate = 2 
        target_rows_per_file = math.ceil(total_rows / num_parts_estimate)

        print(f"Total rows: {total_rows}")
        
        file_count = 1
        current_rows = 0
        base_name = os.path.basename(source_path).replace(".json", "")
        current_output_path = os.path.join(target_dir, f"{base_name}_{file_count}.jsonl")
        f_out = open(current_output_path, 'w', encoding='utf-8')

        for entry in data:
            line = json.dumps(entry, ensure_ascii=False) + "\n"
            line_encoded = line.encode('utf-8')

            # Trigger new file if:
            # 1. We hit the target row count (to keep it balanced)
            # 2. OR we are about to exceed the 95MB byte limit (to keep it safe)
            size_limit_hit = (f_out.tell() + len(line_encoded) > MAX_BYTES)
            balance_limit_hit = (current_rows >= target_rows_per_file)

            if size_limit_hit or balance_limit_hit:
                # Only close and swap if we actually have data in the current file
                if current_rows > 0:
                    f_out.close()
                    actual_size = os.path.getsize(current_output_path) / (1024 * 1024)
                    print(f"✅ Saved {current_output_path} | Rows: {current_rows} | Size: {actual_size:.2f} MB")
                    
                    file_count += 1
                    current_rows = 0
                    current_output_path = os.path.join(target_dir, f"{base_name}_{file_count}.jsonl")
                    f_out = open(current_output_path, 'w', encoding='utf-8')

            f_out.write(line)
            current_rows += 1

        f_out.close()
        actual_size = os.path.getsize(current_output_path) / (1024 * 1024)
        print(f"✅ Saved {current_output_path} | Rows: {current_rows} | Size: {actual_size:.2f} MB")
        print("\n--- Split Complete ---")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    split_balanced_jsonl(input_file, output_dir)