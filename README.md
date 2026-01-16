# Binance Futures Trade Aggregation Pipeline

A production-ready Python pipeline for aggregating Binance Futures trades into positions with P&L calculations. Features per-symbol watermarking for incremental updates, automatic retry logic, connection pooling, and clean modular architecture.

## Overview

This tool automatically:
- **Fetches** your futures trades from Binance API with pagination support (>1000 trades)
- **Aggregates** raw fills into round-trip positions (entry → close)
- **Calculates** realized P&L, commissions, and fees
- **Persists** data incrementally (only fetches new trades since last run)
- **Exports** to CSV for analysis, spreadsheets, or further processing

## Features

✅ **Per-Symbol Watermarking** - Incremental updates, only fetch new trades  
✅ **Pagination Support** - Handles >1000 trades per symbol  
✅ **Fault Tolerance** - Exponential backoff retry with jitter  
✅ **Connection Pooling** - Efficient HTTP session management  
✅ **Atomic Writes** - Safe CSV updates with temp file + atomic rename  
✅ **Complete Coverage** - Fetches both active symbols and historical symbols from CSV  
✅ **Clean Architecture** - Modular design with clear separation of concerns  
✅ **Detailed Logging** - Track pipeline execution and troubleshoot issues  

## Installation

### Prerequisites
- Python 3.9+
- Binance Futures account with API key

### Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/karim-elngr/binance-futures-trade-aggregation-pipeline.git
   cd binance-futures-trade-aggregation-pipeline
   ```

2. **Create virtual environment**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure API credentials**
   
   Create a `.env` file in the project root:
   ```env
   BINANCE_API_KEY=your_api_key_here
   BINANCE_SECRET_KEY=your_secret_key_here
   LOG_LEVEL=INFO
   ```
   
   Get your API key from [Binance API Management](https://www.binance.com/en/account/api-management)

## Usage

### Basic Usage (Incremental - Recommended)

Fetches only trades since last run using watermarks:
```bash
python main.py
```

### Full History (from specific date)

Override watermark to fetch trades from a specific date:
```bash
python main.py 2025-01-01
```

Date format: `YYYY-MM-DD`

## Output Files

### `futures_fills.csv`
Raw trade data from Binance API, appended incrementally:
- `time` - Trade execution timestamp
- `symbol` - Trading pair (e.g., BTCUSDT)
- `side` - BUY or SELL
- `qty` - Trade quantity
- `price` - Trade price
- `commission` - Fee charged
- `commissionAsset` - Asset charged (usually USDT)
- `realizedPnl` - Realized P&L from this fill
- `positionSide` - LONG or SHORT
- `id` - Unique trade ID (used for deduplication)

### `futures_positions.csv`
Aggregated round-trip positions (regenerated each run):
- `status` - OPEN or CLOSED
- `symbol` - Trading pair
- `direction` - LONG or SHORT
- `open_time` - Position entry timestamp
- `close_time` - Position close timestamp (null if open)
- `entry_qty` - Quantity entered
- `entry_vwap` - Volume-weighted average price
- `realizedPnl` - P&L from closed position
- `commission` - Total commissions paid
- `net_pnl_after_fees` - P&L minus all fees
- `fills` - Number of trades in position

### `state.json`
Watermark state for incremental updates:
- `last_trade_time_ms` - Timestamp of last fetched trade
- Per-symbol `last_trade_id` - Last trade ID processed (for pagination)

## Architecture

### Modular Design

```
main.py                 # Orchestrator - coordinates pipeline
├── config.py           # Constants and configuration
├── state_manager.py    # Watermark persistence (JSON)
├── binance_client.py   # Binance API integration
├── aggregator.py       # Position aggregation logic
└── output_writer.py    # CSV output and formatting
```

### Module Responsibilities

| Module | Purpose |
|--------|---------|
| **config.py** | API URLs, file paths, numeric thresholds, logging config |
| **state_manager.py** | Load/save watermarks, manage incremental update state |
| **binance_client.py** | Binance API client, pagination, retry logic, symbol discovery |
| **aggregator.py** | Convert fills to positions, calculate P&L, track state machine |
| **output_writer.py** | Write/append CSV files atomically, format summaries |
| **main.py** | Orchestrate all modules, error handling, logging |

## Configuration

Edit `config.py` to customize:
- `API_BASE_URL` - Binance API endpoint
- `FILLS_CSV` - Path to fills output
- `POSITIONS_CSV` - Path to positions output
- `STATE_FILE` - Path to watermark state
- `MAX_LIMIT` - Max trades per API request (default: 1000)
- `REQUEST_TIMEOUT` - HTTP timeout in seconds (default: 10)
- `MAX_RETRIES` - Max retry attempts (default: 5)
- `LOG_LEVEL` - Logging level (DEBUG, INFO, WARNING, ERROR)

## Examples

### Run daily to get new trades
```bash
# Cron job: fetch new trades every morning
0 8 * * * cd /path/to/project && /path/to/.venv/bin/python main.py
```

### Export specific date range
```bash
# Get all trades from January 2025
python main.py 2025-01-01
```

### View results
```bash
# Check open positions
cat futures_positions.csv | head -20

# Check total P&L
python -c "import pandas as pd; df = pd.read_csv('futures_positions.csv'); print(f'Total P&L: ${df[\"net_pnl_after_fees\"].sum():.2f}')"
```

## Troubleshooting

**No trades fetched**
- Check API credentials in `.env`
- Ensure you have trading history on Binance Futures
- Try running with date override: `python main.py 2025-01-01`

**API rate limit (429 error)**
- Pipeline automatically retries with exponential backoff
- If persistent, increase `MAX_RETRIES` or `REQUEST_TIMEOUT` in config.py

**Permission denied on CSV write**
- Ensure write permissions in current directory
- Check disk space availability

**Import errors**
- Verify virtual environment is activated
- Run `pip install -r requirements.txt` again

## Requirements

```
requests>=2.31.0
pandas>=2.0.0
python-dotenv>=1.0.0
```

## License

This project is licensed under the MIT License - see LICENSE file for details.

## Disclaimer

This tool is provided as-is for educational and personal use. Use at your own risk. Always validate results before making trading decisions. The authors are not responsible for trading losses or errors in calculations.

## Support

For issues, questions, or suggestions:
1. Check the troubleshooting section above
2. Review the module docstrings in the code
3. Open an issue on GitHub
