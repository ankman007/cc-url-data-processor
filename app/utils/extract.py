import os
import requests
import gzip
import shutil  
from paths import get_warc_file_path

download_dir = 'common_crawl_data'
os.makedirs(download_dir, exist_ok=True)

base_url = 'https://data.commoncrawl.org/'

processed_file = 'processed_files.txt'

file_paths = get_warc_file_path()

if os.path.exists(processed_file):
    with open(processed_file, 'r') as f:
        processed_files = set(f.read().splitlines())  
else:
    processed_files = set()

max_files_to_process = 2  
files_processed_in_this_run = 0

with open(processed_file, 'a') as log_file:
    for path in file_paths:
        file_name = os.path.basename(path)

        if file_name in processed_files:
            print(f"Skipping {file_name}, already processed.")
            continue

        try:
            download_url = base_url + path
            local_path = os.path.join(download_dir, file_name)

            print(f"Downloading {file_name}...")
            response = requests.get(download_url, stream=True)
            response.raise_for_status()  

            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)

            print(f"Downloaded {file_name} successfully.")

            # Step 2: Decompress the file
            print(f"Decompressing {file_name}...")
            with gzip.open(local_path, 'rb') as f_in:
                with open(local_path.replace('.gz', '.cdx'), 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)  # Uses shutil to copy the file contents
            print(f"Decompressed {file_name} successfully.")

            # Optional: Remove the compressed file after extraction
            os.remove(local_path)
            print(f"Removed the compressed file {file_name}.")

            # Log this file as processed
            log_file.write(file_name + '\n')

            # Increment the counter
            files_processed_in_this_run += 1

            # Break if the max number of files has been processed
            if files_processed_in_this_run >= max_files_to_process:
                print("Processing limit reached. Exiting.")
                break

        except requests.exceptions.RequestException as e:
            print(f"Failed to download {file_name}: {e}")
        except Exception as e:
            print(f"An error occurred with {file_name}: {e}")