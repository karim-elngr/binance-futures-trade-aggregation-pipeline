"""
Configuration and constants for Binance Futures data pipeline.
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# API Configuration
API_KEY = os.getenv('API_KEY')
SECRET_KEY = os.getenv('SECRET_KEY')
BASE_URL = 'https://fapi.binance.com'

# File Storage
FILLS_CSV = 'futures_fills.csv'
POSITIONS_CSV = 'futures_positions.csv'
STATE_FILE = 'state.json'

# Trade Processing
NUMERIC_COLUMNS = ['price', 'qty', 'realizedPnl', 'quoteQty', 'commission']
POSITION_SIDE_DEFAULT = 'BOTH'
MIN_QTY_THRESHOLD = 1e-12

# API Constants
REQUEST_TIMEOUT = 10
MAX_RETRIES = 5
MAX_LIMIT = 1000

# Logging
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_LEVEL = 'INFO'
