"""
Binance Futures Trade Aggregation Pipeline

Main orchestrator that coordinates:
- API client for fetching trades
- State manager for watermarking
- Aggregator for position calculations
- Output writer for CSV persistence
"""

import sys
import logging
import requests
from typing import Optional
from datetime import datetime

# Import modules
import config
import state_manager
import binance_client
import aggregator
import output_writer

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format=config.LOG_FORMAT
)
logger = logging.getLogger(__name__)


def validate_configuration() -> bool:
    """Validate API credentials are configured."""
    if not config.API_KEY or not config.SECRET_KEY:
        logger.error("API_KEY and SECRET_KEY must be set in .env file")
        return False
    logger.info("✓ API credentials configured")
    return True


def main(start_date: Optional[str] = None) -> int:
    """
    Main entry point for the trading data aggregation pipeline.
    
    Args:
        start_date: Optional date in YYYY-MM-DD format to override watermark
        
    Returns:
        Exit code (0 = success, 1 = failure)
    """
    logger.info("=" * 70)
    logger.info("Binance Futures Trade Aggregation Pipeline")
    logger.info("=" * 70)
    
    try:
        # Validate credentials
        if not validate_configuration():
            return 1
        
        # Load watermark state
        state = state_manager.load_state()
        last_time = state.get('last_trade_time_ms', 0)
        logger.info(f"Loaded watermark: last_trade_time={last_time}")
        
        # Create session for API calls
        session = requests.Session()
        
        try:
            # Fetch active symbols
            logger.info("Fetching active symbols...")
            active_symbols = binance_client.get_active_symbols(session)
            if not active_symbols:
                logger.warning("No active symbols found")
            
            # Build complete symbol list (active + historical)
            symbols = binance_client.get_symbols_to_query(active_symbols)
            if not symbols:
                logger.warning("No symbols to query")
                return 0
            
            # Fetch trades with pagination and retry
            logger.info(f"Fetching trades for {len(symbols)} symbols...")
            trades, updated_state = binance_client.fetch_new_trades(
                session, symbols, start_date, state
            )
            
            if not trades:
                logger.info("No new trades fetched")
                return 0
            
            # Save updated watermark
            state_manager.save_state(updated_state)
            
            # Write raw fills to CSV
            logger.info("Writing raw fills to CSV...")
            total_fills = output_writer.write_fills_csv(trades)
            
            # Aggregate into positions
            logger.info("Aggregating trades into positions...")
            import pandas as pd
            df_fills = pd.read_csv(config.FILLS_CSV)
            df_fills['time'] = pd.to_datetime(df_fills['time'])
            positions_df = aggregator.aggregate_trades_to_positions(df_fills)
            
            # Write positions to CSV
            logger.info("Writing positions to CSV...")
            total_positions = output_writer.write_positions_csv(positions_df)
            
            # Print summary
            summary = output_writer.format_positions_summary(positions_df)
            logger.info(summary)
            
        finally:
            session.close()
        
        logger.info("=" * 70)
        logger.info("✓ Pipeline completed successfully")
        logger.info("=" * 70)
        return 0
    
    except Exception as e:
        logger.exception(f"Pipeline failed with error: {e}")
        logger.info("=" * 70)
        logger.info("✗ Pipeline failed")
        logger.info("=" * 70)
        return 1


if __name__ == "__main__":
    start_date = None
    
    # Parse command-line arguments
    if len(sys.argv) > 1:
        start_date = sys.argv[1]
    
    exit_code = main(start_date)
    sys.exit(exit_code)
