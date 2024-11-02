import json

def process_cdx_file(input_file, limit=None):
    extracted_data = []

    with open(input_file, 'r') as file:
        for i, line in enumerate(file):
            try:
                if limit is not None and i >= limit:
                    break
                
                parts = line.strip().split(' ', 2)
                
                if len(parts) > 2:
                    json_data = parts[2]
                    data_dict = json.loads(json_data)
                    extracted_data.append(data_dict)
            
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e} in line: {line}")
    
    return extracted_data

input_file = 'common_crawl_data/cdx-00000.cdx'
result = process_cdx_file(input_file, limit=10)  
print(result)

