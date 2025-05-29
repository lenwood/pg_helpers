### pg_helpers/config.py
"""Configuration and environment handling"""
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_db_config():
    """Get database configuration from environment variables"""
    return {
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME')
    }

def validate_db_config():
    """Validate that required environment variables are set"""
    config = get_db_config()
    required_keys = ['user', 'password', 'host', 'database']
    
    missing = [key for key in required_keys if not config[key]]
    if missing:
        raise ValueError(f"Missing required environment variables: {missing}")
    
    return config
