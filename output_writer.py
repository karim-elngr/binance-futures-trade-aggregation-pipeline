"""
Output writers for CSV files: raw fills and derived positions.
"""

import os
import logging
from typing import List, Dict

import pandas as pd

import config

logger = logging.getLogger(__name__)


def _atomic_write(filepath: str, df: pd.DataFrame) -> None:
    """
    Atomically write DataFrame to CSV using temp file.
    
    Args:
        filepath: Target CSV file path
        df: DataFrame to write
    """
    tmp = filepath + ".tmp"
    df.to_csv(tmp, index=False)
    os.replace(tmp, filepath)
    logger.info(f"Wrote {len(df)} rows to {filepath}")


def write_fills_csv(new_trades: List[Dict]) -> int:
    """
    Append new trades to raw fills CSV (append-only, immutable).
    
    Args:
        new_trades: List of trade dictionaries from Binance API
        
    Returns:
        Total number of fills in CSV
    """
    if not new_trades:
        logger.info("No new trades to append")
        return 0

    df_new = pd.DataFrame(new_trades)
    df_new['time'] = pd.to_datetime(df_new['time'], unit='ms')
    
    # Load existing fills
    if os.path.exists(config.FILLS_CSV) and os.path.getsize(config.FILLS_CSV) > 0:
        try:
            df_existing = pd.read_csv(config.FILLS_CSV)
            df_existing['id'] = pd.to_numeric(df_existing['id'], errors='coerce')
            # Deduplicate by trade id (keeps only first occurrence)
            combined_df = pd.concat([df_existing, df_new]).drop_duplicates(subset=['id'], keep='first').reset_index(drop=True)
            logger.info(f"Combined with {len(df_existing)} existing fills")
        except Exception as e:
            logger.error(f"Error reading existing fills CSV: {e}. Using new trades only.")
            combined_df = df_new
    else:
        combined_df = df_new

    # Sort by time
    combined_df = combined_df.sort_values('time').reset_index(drop=True)
    
    # Write raw fills (atomic write)
    _atomic_write(config.FILLS_CSV, combined_df)
    logger.info(f"Appended {len(df_new)} new trades. Total fills: {len(combined_df)}")
    
    return len(combined_df)


def write_positions_csv(positions_df: pd.DataFrame) -> int:
    """
    Write derived positions CSV (regenerated from fills).
    
    Args:
        positions_df: DataFrame with aggregated positions
        
    Returns:
        Total number of positions in CSV
    """
    if positions_df.empty:
        logger.warning("No positions to write")
        return 0
    
    # Atomic write
    _atomic_write(config.POSITIONS_CSV, positions_df)
    logger.info(f"Generated {len(positions_df)} positions in {config.POSITIONS_CSV}")
    
    return len(positions_df)


def format_positions_summary(positions_df: pd.DataFrame) -> str:
    """
    Generate human-readable summary of positions.
    
    Args:
        positions_df: DataFrame with aggregated positions
        
    Returns:
        Formatted summary string
    """
    if positions_df.empty:
        return "No positions"
    
    open_count = len(positions_df[positions_df.get('status', 'CLOSED') == 'OPEN'])
    closed_count = len(positions_df[positions_df.get('status', 'CLOSED') == 'CLOSED'])
    
    total_pnl = positions_df['net_pnl_after_fees'].sum()
    total_commission = positions_df['commission'].sum()
    
    summary = f"""
    Positions Summary:
    - Open: {open_count}
    - Closed: {closed_count}
    - Total P&L after fees: ${total_pnl:,.2f}
    - Total Commission: ${total_commission:,.2f}
    """
    
    return summary.strip()
