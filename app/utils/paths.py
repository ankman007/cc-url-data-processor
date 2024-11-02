import os 

def get_warc_file_path() -> list:
    current_dir = os.path.dirname(__file__)
    file_path = os.path.join(current_dir, "cc-index.paths")
    
    with open(file_path, 'r') as file:
        lines = [line.strip() for line in file.readlines()]
        return lines