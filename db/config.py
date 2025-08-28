import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database configuration from environment variables
DB_NAME = os.getenv('DB_NAME', 'argument-mining')
DB_HOST = os.getenv('DB_HOST', 'argumentmining.ddns.net')
DB_PORT = os.getenv('DB_PORT', '3306')
DB_USER = os.getenv('DB_USER', 'root')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')

# Construct database URI
# Use DATABASE_URL if provided, otherwise construct from individual variables
DB_URI = os.getenv('DATABASE_URL')
if not DB_URI:
    # Use PyMySQL by default (pure Python, no C dependencies)
    if DB_PASSWORD:
        DB_URI = f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    else:
        DB_URI = f'mysql+pymysql://{DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

# Cache configuration
CACHE_ENABLED = os.getenv('CACHE_ENABLED', 'True').lower() == 'true'
