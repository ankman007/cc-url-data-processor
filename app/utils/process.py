import json
from loguru import logger
from app.database import execute_query

def process_cdx_file(input_file, limit=None):
    logger.info(f"Starting to process CDX file: {input_file}")

    extracted_data = []

    try:
        with open(input_file, 'r') as file:
            for i, line in enumerate(file):
                if limit is not None and i >= limit:
                    logger.info(f"Limit of {limit} reached; stopping processing.")
                    break

                parts = line.strip().split(' ', 2)

                if len(parts) > 2:
                    json_data = parts[2]
                    try:
                        data_dict = json.loads(json_data)
                        extracted_data.append(data_dict)
                        logger.debug(f"Extracted data from line {i}: {data_dict}")
                    except json.JSONDecodeError as e:
                        logger.error(f"Error decoding JSON in line {i}: {e} - line content: {line}")

    except FileNotFoundError:
        logger.error(f"File not found: {input_file}")
        return
    except Exception as e:
        logger.error(f"An error occurred while processing the file: {e}")
        return

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
                logger.info(f"Inserted/updated metadata for URL: {url}")
            except Exception as e:
                logger.error(f"Error inserting data for URL: {url} - {e}")

    logger.info(f"Inserted {len(extracted_data)} records into the database.")
