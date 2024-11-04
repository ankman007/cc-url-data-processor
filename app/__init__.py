from fastapi import FastAPI
from app.database import create_table
from app.utils.download import download_and_extract_cdx_files
from app.utils.process import process_cdx_directory
import os

def create_app():
    current_dir = os.path.dirname(__file__)
    file_path = os.path.join(current_dir, "common_crawl_data/cdx-00000.cdx")
    
    app = FastAPI()
    
    create_table()
    download_and_extract_cdx_files()
    process_cdx_directory('common_crawl_data')
    
    from app.views import router as todo_router
    app.include_router(todo_router, prefix='')
    return app 