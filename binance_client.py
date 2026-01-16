"""
Binance Futures API client with retry logic and connection pooling.
"""

import time
import hmac
import hashlib
import random
import logging
import os
from typing import List, Dict, Tuple, Optional

import requests

import config
import state_manager

logger = logging.getLogger(__name__)


def get_binance_signature(data: str, secret: str) -> str:
    """
    Generate HMAC SHA256 signature for Binance API requests.
    
    Args:
        data: Query string to sign
        secret: Secret key for signing
        
    Returns:
        Hex-encoded HMAC SHA256 signature
    """
    return hmac.new(secret.encode('utf-8'), data.encode('utf-8'), hashlib.sha256).hexdigest()


def request_with_retry(session: requests.Session, method: str, url: str, 
                       headers: Dict, timeout: int = config.REQUEST_TIMEOUT, 
                       max_retries: int = config.MAX_RETRIES) -> requests.Response:
    """
    Execute HTTP request with exponential backoff retry on rate limits and server errors.
    
    Args:
        session: Requests session object
        method: HTTP method (GET, POST, etc.)
        url: Request URL
        headers: Request headers
        timeout: Request timeout in seconds
        max_retries: Maximum number of retry attempts
        
    Returns:
        Response object
        
    Raises:
        RuntimeError: If request fails after all retries
    """
    for attempt in range(max_retries):
        try:
            resp = session.request(method, url, headers=headers, timeout=timeout)
            
            # Retryable errors: rate limit, IP ban, or server error
            if resp.status_code in (429, 418) or resp.status_code >= 500:
                sleep_s = min(60, (2 ** attempt) + random.uniform(0, 1))
                logger.warning(f"Status {resp.status_code}. Retry in {sleep_s:.2f}s (attempt {attempt + 1}/{max_retries})")
                time.sleep(sleep_s)
                continue
            
            resp.raise_for_status()
            return resp
            
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                sleep_s = min(60, (2 ** attempt) + random.uniform(0, 1))
                logger.warning(f"Request error: {e}. Retry in {sleep_s:.2f}s (attempt {attempt + 1}/{max_retries})")
                time.sleep(sleep_s)
            else:
                raise RuntimeError(f"Request failed after {max_retries} retries: {url}") from e
    
    raise RuntimeError(f"Request failed after {max_retries} retries: {url}")


def get_active_symbols(session: requests.Session) -> List[str]:
    """
    Fetch symbols with open positions from Binance.
    
    Args:
        session: Requests session for API calls
        
    Returns:
        List of symbol strings with non-zero positions
    """
    try:
        endpoint = '/fapi/v2/positionRisk'
        params = {'timestamp': int(time.time() * 1000)}
        query_string = '&'.join([f"{k}={v}" for k, v in params.items()])
        signature = get_binance_signature(query_string, config.SECRET_KEY)
        url = f"{config.BASE_URL}{endpoint}?{query_string}&signature={signature}"
        headers = {'X-MBX-APIKEY': config.API_KEY}

        resp = request_with_retry(session, 'GET', url, headers)
        positions = resp.json()
        symbols = [pos['symbol'] for pos in positions if float(pos['positionAmt']) != 0]
        logger.info(f"Found {len(symbols)} symbols with open positions")
        return symbols
        
    except Exception as e:
        logger.error(f"Error fetching active positions: {e}")
        return []


def get_symbols_to_query(active_symbols: List[str]) -> List[str]:
    """
    Build complete symbol list from active symbols + historical symbols from CSV.
    
    This ensures we don't miss fills from recently closed positions.
    
    Args:
        active_symbols: Currently active symbols with open positions
        
    Returns:
        Sorted list of all symbols to query
    """
    import pandas as pd
    
    symbols = set(active_symbols)
    
    # Add historical symbols from fills CSV
    if os.path.exists(config.FILLS_CSV) and os.path.getsize(config.FILLS_CSV) > 0:
        try:
            df = pd.read_csv(config.FILLS_CSV, usecols=["symbol"])
            historical = set(df["symbol"].dropna().unique())
            symbols |= historical
            logger.info(f"Added {len(historical)} historical symbols from CSV")
        except Exception as e:
            logger.warning(f"Could not load historical symbols: {e}")
    
    result = sorted(symbols)
    logger.info(f"Total symbols to query: {len(result)}")
    return result


def fetch_new_trades(session: requests.Session, symbols: List[str], 
                     start_date: Optional[str] = None, state: Optional[Dict] = None) -> Tuple[List[Dict], Dict]:
    """
    Fetch new futures trades from Binance with pagination and time-based watermarking.
    
    Uses startTime watermark to avoid missing trades. Handles pagination for >1000 trades per symbol.
    
    Args:
        session: Requests session for API calls
        symbols: List of symbols to query
        start_date: Optional date in YYYY-MM-DD format to override watermark
        state: Current state dict with watermark info
        
    Returns:
        Tuple of (list of trades, updated state dict)
    """
    from datetime import datetime
    
    if state is None:
        state = state_manager.load_state()
    
    all_trades = []
    start_time_ms = None
    
    # Override watermark if explicit date provided
    if start_date:
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            start_time_ms = int(start_dt.timestamp() * 1000)
            logger.info(f"Overriding watermark to {start_date}")
        except ValueError:
            logger.error(f"Invalid date format. Use YYYY-MM-DD. Got: {start_date}")
            return [], state
    else:
        start_time_ms = state.get("last_trade_time_ms", 0)
    
    endpoint = '/fapi/v1/userTrades'
    
    for symbol in symbols:
        try:
            trades_for_symbol = []
            from_id = state.get("last_id_by_symbol", {}).get(symbol, 0)
            page = 0
            
            # Pagination loop: fetch until no more trades
            while True:
                page += 1
                params = {
                    'symbol': symbol,
                    'limit': config.MAX_LIMIT,
                    'timestamp': int(time.time() * 1000)
                }
                
                # Use fromId for pagination if available
                if from_id > 0:
                    params['fromId'] = from_id + 1
                # Otherwise use time watermark
                elif start_time_ms > 0:
                    params['startTime'] = start_time_ms
                
                query_string = '&'.join([f"{k}={v}" for k, v in params.items()])
                signature = get_binance_signature(query_string, config.SECRET_KEY)
                url = f"{config.BASE_URL}{endpoint}?{query_string}&signature={signature}"
                headers = {'X-MBX-APIKEY': config.API_KEY}
                
                resp = request_with_retry(session, 'GET', url, headers)
                trades = resp.json()
                
                if not trades:
                    logger.debug(f"{symbol} page {page}: no more trades")
                    break
                
                # Filter trades by start_time if using time-based pagination
                if start_time_ms > 0 and from_id == 0:
                    trades = [t for t in trades if t['time'] >= start_time_ms]
                
                trades_for_symbol.extend(trades)
                from_id = trades[-1]['id']
                
                logger.debug(f"{symbol} page {page}: fetched {len(trades)} trades (fromId: {from_id})")
                
                # Stop if less than limit (means we got all available)
                if len(trades) < config.MAX_LIMIT:
                    break
            
            if trades_for_symbol:
                all_trades.extend(trades_for_symbol)
                state = state_manager.update_watermark(state, trades_for_symbol)
                logger.info(f"{symbol}: {len(trades_for_symbol)} trades")
            
        except Exception as e:
            logger.error(f"Error fetching {symbol}: {e}")
            continue
    
    logger.info(f"Total trades fetched: {len(all_trades)}")
    return all_trades, state
