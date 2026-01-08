import os
from dotenv import load_dotenv

load_dotenv()


def get_db_dsn() -> str:
    """Build PostgreSQL connection string from environment variables."""
    db_name = os.getenv("POSTGRES_DB")
    db_user = os.getenv("POSTGRES_USER")
    db_password = os.getenv("POSTGRES_PASSWORD")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    
    if not db_name:
        raise ValueError("POSTGRES_DB environment variable is required")
    if not db_user:
        raise ValueError("POSTGRES_USER environment variable is required")
    if not db_password:
        raise ValueError("POSTGRES_PASSWORD environment variable is required")
    
    return f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

