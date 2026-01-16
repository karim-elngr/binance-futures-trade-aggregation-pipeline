"""
Trade aggregation: converts raw fills into position round-trips with P&L metrics.
"""

import logging
from typing import Dict, Tuple, List

import pandas as pd

import config

logger = logging.getLogger(__name__)


def _signed_qty(row: pd.Series) -> float:
    """
    Convert BUY/SELL into signed quantity relative to positionSide.
    
    For hedge mode (LONG/SHORT): BUY increases LONG, SELL increases SHORT.
    For BOTH (net mode): BUY is positive, SELL is negative.
    
    Args:
        row: Trade row from dataframe
        
    Returns:
        Signed quantity
    """
    qty = float(row["qty"])
    side = row["side"]
    ps = row.get("positionSide", config.POSITION_SIDE_DEFAULT)

    if ps == "LONG":
        return qty if side == "BUY" else -qty
    elif ps == "SHORT":
        return qty if side == "SELL" else -qty
    else:
        return qty if side == "BUY" else -qty


def aggregate_trades_to_positions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate raw trades (fills) into position round-trips (open -> close).
    
    Input: raw trades from /fapi/v1/userTrades
    Output: one row per position round-trip with entry/exit metrics
    
    Args:
        df: DataFrame with raw trades
        
    Returns:
        DataFrame with aggregated positions
    """
    df = df.copy()

    # Normalize types
    df["time"] = pd.to_datetime(df["time"])
    for col in config.NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    # Ensure stable ordering
    df = df.sort_values(["symbol", "positionSide", "time", "id"]).reset_index(drop=True)

    positions = []
    state = {}

    def start_state(key: Tuple, first_row: pd.Series, signed_q: float) -> None:
        """Initialize a new position state."""
        state[key] = {
            "symbol": first_row["symbol"],
            "positionSide": first_row.get("positionSide", config.POSITION_SIDE_DEFAULT),
            "open_time": first_row["time"],
            "close_time": pd.NaT,
            "direction": "LONG" if signed_q > 0 else "SHORT",
            "qty_opened": 0.0,
            "entry_notional": 0.0,
            "max_abs_qty": 0.0,
            "fills": 0,
            "commission": 0.0,
            "realizedPnl": 0.0,
            "net_qty": 0.0,
        }

    def flush_if_closed(key: Tuple) -> None:
        """If position is closed, append to positions list."""
        st = state[key]
        if abs(st["net_qty"]) < config.MIN_QTY_THRESHOLD:
            entry_qty = abs(st["qty_opened"])
            entry_vwap = (st["entry_notional"] / entry_qty) if entry_qty > 0 else 0.0

            positions.append({
                "symbol": st["symbol"],
                "positionSide": st["positionSide"],
                "direction": st["direction"],
                "open_time": st["open_time"],
                "close_time": st["close_time"],
                "duration_min": (st["close_time"] - st["open_time"]).total_seconds() / 60.0
                                if pd.notna(st["close_time"]) else None,
                "max_position_qty": st["max_abs_qty"],
                "entry_qty": entry_qty,
                "entry_vwap": entry_vwap,
                "realizedPnl": st["realizedPnl"],
                "commission": st["commission"],
                "net_pnl_after_fees": st["realizedPnl"] - st["commission"],
                "fills": st["fills"],
            })
            del state[key]

    # Process each trade
    for _, row in df.iterrows():
        key = (row["symbol"], row.get("positionSide", config.POSITION_SIDE_DEFAULT))
        signed_q = _signed_qty(row)
        price = float(row["price"])
        commission = float(row["commission"])
        realized = float(row["realizedPnl"])

        # Initialize state if needed
        if key not in state:
            start_state(key, row, signed_q)

        st = state[key]

        # If state is flat, restart
        if abs(st["net_qty"]) < config.MIN_QTY_THRESHOLD:
            start_state(key, row, signed_q)
            st = state[key]

        st["fills"] += 1
        st["commission"] += commission
        st["realizedPnl"] += realized

        prev_net = st["net_qty"]
        new_net = prev_net + signed_q

        # Update entry VWAP for same-direction fills
        opening_sign = 1 if st["direction"] == "LONG" else -1
        if opening_sign * signed_q > 0:
            st["qty_opened"] += signed_q
            st["entry_notional"] += price * abs(signed_q)

        st["net_qty"] = new_net
        st["max_abs_qty"] = max(st["max_abs_qty"], abs(new_net))

        # Handle position flips (crossing zero)
        if (prev_net > 0 and new_net < 0) or (prev_net < 0 and new_net > 0):
            st["close_time"] = row["time"]
            st["net_qty"] = 0.0
            flush_if_closed(key)

            # Start new position with leftover
            leftover = new_net
            start_state(key, row, leftover)
            st2 = state[key]
            st2["fills"] = 1
            st2["commission"] = 0.0
            st2["realizedPnl"] = 0.0
            st2["net_qty"] = leftover
            st2["qty_opened"] = leftover
            st2["entry_notional"] = price * abs(leftover)
            st2["max_abs_qty"] = abs(leftover)

        # Normal close (reaching zero)
        if abs(st["net_qty"]) < config.MIN_QTY_THRESHOLD:
            st["close_time"] = row["time"]
            flush_if_closed(key)

    # Handle open positions
    open_positions = []
    for key, st in list(state.items()):
        entry_qty = abs(st["qty_opened"])
        entry_vwap = (st["entry_notional"] / entry_qty) if entry_qty > 0 else 0.0
        open_positions.append({
            "symbol": st["symbol"],
            "positionSide": st["positionSide"],
            "direction": st["direction"],
            "open_time": st["open_time"],
            "close_time": pd.NaT,
            "duration_min": None,
            "max_position_qty": st["max_abs_qty"],
            "entry_qty": entry_qty,
            "entry_vwap": entry_vwap,
            "realizedPnl": st["realizedPnl"],
            "commission": st["commission"],
            "net_pnl_after_fees": st["realizedPnl"] - st["commission"],
            "fills": st["fills"],
            "status": "OPEN",
            "net_qty": st["net_qty"],
        })

    # Combine closed and open positions
    positions_df = pd.DataFrame(positions)
    open_df = pd.DataFrame(open_positions)

    if not open_df.empty:
        positions_df["status"] = "CLOSED"
        positions_df = pd.concat([positions_df, open_df], ignore_index=True)

    result = positions_df.sort_values(["open_time"]).reset_index(drop=True)
    logger.info(f"Aggregated into {len(result)} positions ({len(positions)} closed, {len(open_positions)} open)")
    return result
