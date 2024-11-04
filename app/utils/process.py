import os, psycopg2
import json
from loguru import logger
from psycopg2.extras import execute_batch
from app.database import get_connection

def insert_extracted_data(extracted_data):
    insert_query = """
    INSERT INTO url_metadata (url, metadata)
    VALUES (%s, %s)
    """
    # ON CONFLICT (url) DO UPDATE
    # SET metadata = EXCLUDED.metadata;
    
    records_to_insert = []
    for entry in extracted_data:
        url = entry.get("url")
        if url:
            metadata = {key: value for key, value in entry.items() if key != "url"}
            records_to_insert.append((url, json.dumps(metadata)))

    if not records_to_insert:
        logger.info("No records to insert.")
        return

    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                execute_batch(cursor, insert_query, records_to_insert)
            conn.commit()
        logger.info(f"Batch insert successful. {len(records_to_insert)} records inserted/updated.")
    except Exception as e:
        logger.error(f"Error during batch insert: {e}")

def parse_cdx_line(line):
    parts = line.strip().split(' ', 2)
    if len(parts) > 2:
        try:
            return json.loads(parts[2])
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON: {e} - line content: {line}")
    return None

def process_cdx_file(input_file, limit=None):
    logger.info(f"Starting to process CDX file: {input_file}")
    extracted_data = []

    try:
        with open(input_file, 'r') as file:
            for i, line in enumerate(file):
                if limit is not None and i >= limit:
                    logger.info(f"Limit of {limit} reached for file {input_file}; stopping processing.")
                    break
                data = parse_cdx_line(line)
                if data:
                    extracted_data.append(data)
                    # logger.debug(f"Extracted data from line {i}: {data}")
                    
    except FileNotFoundError:
        logger.error(f"File not found: {input_file}")
        return
    except Exception as e:
        logger.error(f"An error occurred while processing the file: {e}")
        return

    insert_extracted_data(extracted_data)
    # return extracted_data

def process_cdx_directory(directory, limit_per_file=100):
    for filename in os.listdir(directory):
        if filename.endswith('.cdx'):
            filepath = os.path.join(directory, filename)
            process_cdx_file(filepath, limit=limit_per_file)

process_cdx_directory('common_crawl_data')