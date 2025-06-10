import os
import sys

def check_file(file_path):
    try:
        # Check if file exists and get its size
        if not os.path.exists(file_path):
            print(f"Error: File not found: {file_path}")
            return
            
        file_size = os.path.getsize(file_path)
        print(f"File: {file_path}")
        print(f"Size: {file_size} bytes")
        print("\nFirst 5 lines:")
        print("-" * 50)
        
        # Read and print first 5 lines
        with open(file_path, 'r') as f:
            for i, line in enumerate(f):
                if i >= 5:
                    break
                print(f"{i+1}: {line.strip()}")
                
        # Print file format information
        print("\nFile format analysis:")
        print("-" * 50)
        with open(file_path, 'r') as f:
            first_line = f.readline().strip()
            if ':' in first_line and '{' in first_line and '}' in first_line:
                print("Format: Matches expected format (e.g., '1: {2, 3, 5}')")
            else:
                print("Format: Does not match expected format")
                print("First line content:", first_line)
                
    except Exception as e:
        print(f"Error checking file: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        check_file(sys.argv[1])
    else:
        print("Please provide a file path as an argument")
