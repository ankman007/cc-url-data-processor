from fastapi import FastAPI
from app.database import create_table
from app.utils.download import download_and_extract_cdx_files
from app.utils.process import process_cdx_directory

def create_app():    
    app = FastAPI()
    
    create_table()
    download_and_extract_cdx_files()
    process_cdx_directory('common_crawl_data')
    
    from app.views import router as todo_router
    app.include_router(todo_router, prefix='')
    return app 