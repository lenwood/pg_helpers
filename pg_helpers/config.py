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
        'database': os.getenv('DB_NAME'),
        'ssl_mode': os.getenv('DB_SSL_MODE', 'require'),
        'ssl_ca_cert': os.getenv('DB_SSL_CA_CERT'),  # Optional CA certificate path
        'ssl_cert': os.getenv('DB_SSL_CERT'),        # Optional client certificate
        'ssl_key': os.getenv('DB_SSL_KEY')           # Optional client key
    }

def validate_db_config():
    """Validate that required environment variables are set"""
    config = get_db_config()
    required_keys = ['user', 'password', 'host', 'database']
    
    missing = [key for key in required_keys if not config[key]]
    if missing:
        raise ValueError(f"Missing required environment variables: {missing}")
    
    # Validate SSL certificate path if provided
    if config['ssl_ca_cert'] and not os.path.exists(config['ssl_ca_cert']):
        raise ValueError(f"SSL CA certificate file not found: {config['ssl_ca_cert']}")
    
    if config['ssl_cert'] and not os.path.exists(config['ssl_cert']):
        raise ValueError(f"SSL client certificate file not found: {config['ssl_cert']}")
    
    if config['ssl_key'] and not os.path.exists(config['ssl_key']):
        raise ValueError(f"SSL client key file not found: {config['ssl_key']}")
    
    return config

def get_ssl_params():
    """Get SSL parameters for connection string"""
    config = get_db_config()
    ssl_params = []
    
    # Always include SSL mode
    ssl_params.append(f"sslmode={config['ssl_mode']}")
    
    # Add CA certificate if provided
    if config['ssl_ca_cert']:
        ssl_params.append(f"sslrootcert={config['ssl_ca_cert']}")
    
    # Add client certificate if provided
    if config['ssl_cert']:
        ssl_params.append(f"sslcert={config['ssl_cert']}")
    
    # Add client key if provided
    if config['ssl_key']:
        ssl_params.append(f"sslkey={config['ssl_key']}")
    
    return '&'.join(ssl_params)
