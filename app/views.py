from loguru import logger
from app.database import execute_query
from fastapi import APIRouter
from psycopg2 import DatabaseError

router = APIRouter()

@router.get('/get-url-metadata/{url}')
def get_url_metadata(url: str):
    query = """
    SELECT url, metadata 
    FROM url_metadata 
    WHERE url = %s;"""
    try:
        result = execute_query(query, url)
        return result
    except DatabaseError as e:
        logger.error(f"Could not fetch data for {url}", e)
    
