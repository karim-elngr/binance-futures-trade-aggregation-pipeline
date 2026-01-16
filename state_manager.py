"""
State management and persistence for tracking trade watermarks.
"""

import json
import os
import logging
from typing import Dict

import config

logger = logging.getLogger(__name__)


def load_state() -> Dict:
    """
    Load watermark state from JSON file.
    
    Returns:
        Dictionary with last_trade_time_ms and last_id_by_symbol
    """
    if os.path.exists(config.STATE_FILE):
        try:
            with open(config.STATE_FILE, "r") as f:
                state = json.load(f)
                logger.debug(f"Loaded state: {state}")
                return state
        except Exception as e:
            logger.warning(f"Could not load state file: {e}")
    
    return {"last_trade_time_ms": 0, "last_id_by_symbol": {}}


def save_state(state: Dict) -> None:
    """
    Atomically save watermark state to JSON file using temp file.
    
    Args:
        state: State dictionary to persist
    """
    try:
        tmp = config.STATE_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(state, f, indent=2)
        os.replace(tmp, config.STATE_FILE)
        logger.debug(f"State saved: {state}")
    except Exception as e:
        logger.error(f"Failed to save state: {e}")


def get_last_trade_time(state: Dict) -> int:
    """Get last recorded trade timestamp in milliseconds."""
    return state.get("last_trade_time_ms", 0)


def get_symbol_last_id(state: Dict, symbol: str) -> int:
    """Get last recorded trade ID for a specific symbol."""
    return state.get("last_id_by_symbol", {}).get(symbol, 0)


def update_watermark(state: Dict, trades: list) -> Dict:
    """
    Update state watermark with new trades.
    
    Args:
        state: Current state dictionary
        trades: List of trade dictionaries
        
    Returns:
        Updated state dictionary
    """
    if not trades:
        return state
    
    # Update time watermark
    max_time = max(t['time'] for t in trades)
    state["last_trade_time_ms"] = max(state.get("last_trade_time_ms", 0), max_time)
    
    # Update per-symbol ID watermark
    if "last_id_by_symbol" not in state:
        state["last_id_by_symbol"] = {}
    
    for symbol in set(t['symbol'] for t in trades):
        symbol_trades = [t for t in trades if t['symbol'] == symbol]
        max_id = max(t['id'] for t in symbol_trades)
        state["last_id_by_symbol"][symbol] = max_id
    
    return state
