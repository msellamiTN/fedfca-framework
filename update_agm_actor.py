def update_agm_actor():
    file_path = "d:/Research2023/FFCA/FedFCA/fedfac-framework.v4/src/server/agmactor_new.py"
    temp_file = "d:/Research2023/FFCA/FedFCA/fedfac-framework.v4/src/server/agmactor_new.py.tmp"
    
    # Read the original file
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # Find the start and end of the _handle_lattice_result method
    start_idx = -1
    end_idx = -1
    in_method = False
    
    for i, line in enumerate(lines):
        if '_handle_lattice_result' in line and 'def ' in line:
            start_idx = i
            in_method = True
        elif in_method and line.strip() == '':
            end_idx = i
            in_method = False
    
    if start_idx == -1:
        print("Could not find _handle_lattice_result method")
        return
    
    if end_idx == -1:
        end_idx = len(lines) - 1
    
    # Read the new implementation
    with open(temp_file, 'r', encoding='utf-8') as f:
        new_implementation = f.read()
    
    # Replace the method
    new_lines = lines[:start_idx] + [new_implementation] + lines[end_idx:]
    
    # Write back to file
    with open(file_path, 'w', encoding='utf-8') as f:
        f.writelines(new_lines)
    
    print("Successfully updated _handle_lattice_result method")

if __name__ == "__main__":
    update_agm_actor()
