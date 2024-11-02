from utils.process import process_cdx_file
from database import execute_query
import json

def insert_url_metadata(file_path, limit=10):
    extracted_data = process_cdx_file(file_path, limit=limit)
    
    insert_query = """
    INSERT INTO url_metadata (url, metadata)
    VALUES (%s, %s)
    ON CONFLICT (url) DO UPDATE
    SET metadata = EXCLUDED.metadata;
    """
    
    for entry in extracted_data:
        url = entry.get("url")
        if url:
            metadata = json.dumps(entry)  
            try:
                execute_query(insert_query, (url, metadata))
            except Exception as e:
                print(f"Error inserting data for URL: {url}, {e}")
    
    print(f"Inserted {len(extracted_data)} records into the database.")

file_path = 'common_crawl_data/cdx-00000.cdx'
insert_url_metadata(file_path, limit=5)
