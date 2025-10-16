import os
from urllib.parse import quote
from sqlalchemy import create_engine
from dotenv import load_dotenv
 
def marcommdb_connection():
    # Load environment variables
    load_dotenv(override=True) 
 
    # Get credentials from environment variables
    username = os.getenv("PG_USERNAME")
    password = os.getenv("PG_PASSWORD")
    host = os.getenv("PG_HOST")
    port = os.getenv("PG_PORT")
    database = os.getenv("PG_DATABASE_EXPORT")
 
    # Ensure all credentials are available
    if not all([username, password, host, port, database]):
        raise ValueError("Missing one or more PostgreSQL environment variables!")
 
    # Encode password to handle special characters
    encoded_password = quote(password, safe="") if password else ""
 
    # Construct PostgreSQL connection string
    DATABASE_URL = f"postgresql+psycopg2://{username}:{encoded_password}@{host}:{port}/{database}"
 
    # Create and return SQLAlchemy engine
    return create_engine(DATABASE_URL)
 