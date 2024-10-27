from loguru import logger
import traceback
from app.models import ToDo 
from app.database import get_connection, create_table
from fastapi import APIRouter, HTTPException

router = APIRouter()

@router.get('/')
def index():
    try:
        conn = get_connection()
        detail = "Database connection made successfully." if conn else "Encountered error when connecting to database."
        create_table(conn)
        return {"message": detail}
    except Exception as e:
        logger.error(f"Error occurred: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail="Internal server error")