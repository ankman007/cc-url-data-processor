from fastapi import FastAPI
from app.database import create_table
from app.utils.extract import download_and_extract_cdx_files
from app.utils.process import process_cdx_file

def create_app():
    app = FastAPI()
    
    create_table()
    download_and_extract_cdx_files()
    process_cdx_file()
    
    from app.views import router as todo_router
    app.include_router(todo_router, prefix='')
    return app 