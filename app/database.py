import os
import psycopg2
from pathlib import Path
from loguru import logger
from dotenv import load_dotenv

env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

DATABASE_URL = os.getenv("DATABASE_URL")

def get_connection():
    try: 
        conn = psycopg2.connect(DATABASE_URL)
        logger.info("Database connection established successfully.")
        return conn 
    
    except psycopg2.OperationalError as e:
        logger.error("Encountered error when establishing database connection.", e)
        raise 

def execute_query(query, params=None):
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            if query.strip().upper().startswith("SELECT"):
                return cursor.fetchall()
            conn.commit()

def create_table():
    create_table_query = """
    CREATE TABLE IF NOT EXISTS todo_db (
    todo_id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    completed BOOLEAN NOT NULL DEFAULT FALSE
    );
    """
    try:
        execute_query(create_table_query)
        logger.info("Table created successfully.")
    except Exception as e: 
        logger.error("Error creating table", e)
        
    
    
    
