import os

DB = "argument-mining"
HOST = "argumentmining.ddns.net:3306"
USER = "guidedproject"
PW = "guidedproject"

# Use PyMySQL by default (pure Python, no C dependencies)
# Can be overridden by DATABASE_URL environment variable
DB_URI = os.getenv('DATABASE_URL', f'mysql+pymysql://{USER}:{PW}@{HOST}/{DB}')
CACHE_ENABLED = True