from loguru import logger
from app.database import get_connection
from fastapi import APIRouter, HTTPException
from psycopg2 import DatabaseError

router = APIRouter()

@router.get('/{url}')
async def get_url_metadata(url: str):
    query = """
    SELECT url, metadata 
    FROM url_metadata 
    WHERE url = %s;
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (url,))
                results = cursor.fetchall()  

        if results:
            return [{"url": row[0], "metadata": row[1]} for row in results]
        else:
            raise HTTPException(status_code=404, detail="URL metadata not found.")
    
    except DatabaseError as e:
        logger.error(f"Could not fetch data for {url}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error.")
