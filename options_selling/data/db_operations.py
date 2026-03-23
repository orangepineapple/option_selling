import psycopg2
import psycopg2.extras
from datetime import date
from dataclasses import dataclass
from typing import Optional
from trading_util.data_util import Bar, HistoricalVolatility

# =============================================================
# Data classes
# =============================================================


@dataclass
class IVBar:
    date    : date
    close_iv: float


@dataclass
class HVValues:
    '''
    '''
    hv_60      : Optional[float]
    hv_252     : Optional[float]
    iv_52w_high: Optional[float]
    iv_52w_low : Optional[float]


# =============================================================
# Ticker operations
# =============================================================

def upsert_ticker(conn, symbol: str) -> int:
    '''
    Insert ticker if it does not exist, return its id either way.

    Returns:
        ticker id (int)
    '''
    sql = '''
        INSERT INTO tickers (symbol)
        VALUES (%s)
        ON CONFLICT (symbol) DO NOTHING
        RETURNING id;
    '''
    with conn.cursor() as cur:
        cur.execute(sql, (symbol.upper(),))
        row = cur.fetchone()

        if row is not None:
            conn.commit()
            return row[0]

        # Already existed — fetch existing id
        cur.execute('SELECT id FROM tickers WHERE symbol = %s', (symbol.upper(),))
        return cur.fetchone()[0]


def get_ticker_id(conn, symbol: str) -> Optional[int]:
    '''
    Return the id for a ticker symbol, or None if it does not exist.
    '''
    with conn.cursor() as cur:
        cur.execute('SELECT id FROM tickers WHERE symbol = %s', (symbol.upper(),))
        row = cur.fetchone()
        return row[0] if row else None


def ticker_exists(conn, symbol: str) -> bool:
    ''' Return True if the ticker is already tracked '''
    return get_ticker_id(conn, symbol) is not None


def remove_ticker(conn, symbol: str) -> bool:
    '''
    Remove a ticker and ALL associated data (price_bars, iv_bars,
    hv_values) via CASCADE. Returns True if deleted, False if not found.
    '''
    with conn.cursor() as cur:
        cur.execute(
            'DELETE FROM tickers WHERE symbol = %s RETURNING id',
            (symbol.upper(),)
        )
        deleted = cur.fetchone()
    conn.commit()
    return deleted is not None

def get_universe(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute('SELECT symbol FROM tickers;')
        rows = cur.fetchall()
    return [row[0] for row in rows]


# =============================================================
# Price bar operations
# =============================================================

def upsert_price_bars(conn, symbol: str, bars: list[Bar]) -> int:
    '''
    Upsert a list of price bars for a ticker.
    Safe to call with overlapping date ranges — existing rows are overwritten.

    Returns:
        number of rows affected
    '''
    ticker_id = upsert_ticker(conn, symbol)

    sql = '''
        INSERT INTO price_bars
            (ticker_id, date, open, high, low, close, volume, num_trades)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker_id, date) DO UPDATE SET
            open       = EXCLUDED.open,
            high       = EXCLUDED.high,
            low        = EXCLUDED.low,
            close      = EXCLUDED.close,
            volume     = EXCLUDED.volume,
            num_trades = EXCLUDED.num_trades;
    '''

    rows = [
        (ticker_id, b.date, b.open, b.high, b.low, b.close, b.volume, b.num_trades)
        for b in bars
    ]

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, rows)
        count = cur.rowcount
    conn.commit()
    return count


# =============================================================
# IV bar operations
# =============================================================

def upsert_iv_bars(conn, symbol: str, bars: list[Bar]) -> int:
    '''
    Upsert a list of IV bars for a ticker.

    Returns:
        number of rows affected
    '''
    ticker_id = upsert_ticker(conn, symbol)

    sql = '''
        INSERT INTO iv_bars (ticker_id, date, close_iv)
        VALUES (%s, %s, %s)
        ON CONFLICT (ticker_id, date) DO UPDATE SET
            close_iv = EXCLUDED.close_iv;
    '''

    rows = [(ticker_id, b.date, b.close) for b in bars]

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, rows)
        count = cur.rowcount
    conn.commit()
    return count


# =============================================================
# HV values operations
# =============================================================

def upsert_hv_values(conn, symbol: str, hv: HistoricalVolatility) -> None:
    '''
    Upsert the nightly HV and 52-week IV bounds for a ticker.
    One row per ticker — fully replaced on each batch run.
    '''
    ticker_id = upsert_ticker(conn, symbol)

    sql = '''
        INSERT INTO hv_values
            (ticker_id, hv_60, hv_252, iv_52w_high, iv_52w_low, calculated_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
        ON CONFLICT (ticker_id) DO UPDATE SET
            hv_60         = EXCLUDED.hv_60,
            hv_252        = EXCLUDED.hv_252,
            iv_52w_high   = EXCLUDED.iv_52w_high,
            iv_52w_low    = EXCLUDED.iv_52w_low,
            calculated_at = NOW();
    '''

    with conn.cursor() as cur:
        cur.execute(sql, (
            ticker_id,
            hv.hv_60,
            hv.hv_252,
            hv.iv_52w_high,
            hv.iv_52w_low
        ))
    conn.commit()


# =============================================================
# Fetch bars for recalculation
# =============================================================

def fetch_price_bars(conn, symbol: str, limit: int = 252) -> list[Bar]:
    '''
    Fetch the most recent `limit` price bars, oldest first.
    Pass directly into calculate_historical_volatility().
    Returns empty list if ticker not found.
    '''
    ticker_id = get_ticker_id(conn, symbol)
    if ticker_id is None:
        return []

    sql = '''
        SELECT date, open, high, low, close, volume, num_trades
        FROM price_bars
        WHERE ticker_id = %s
        ORDER BY date DESC
        LIMIT %s;
    '''

    with conn.cursor() as cur:
        cur.execute(sql, (ticker_id, limit))
        rows = cur.fetchall()

    # Reverse so caller gets oldest first — required by Yang-Zhang
    return [
        Bar(
            date       = row[0],
            open       = float(row[1]),
            high       = float(row[2]),
            low        = float(row[3]),
            close      = float(row[4]),
            volume     = row[5],
            num_trades = row[6]
        )
        for row in reversed(rows)
    ]


def fetch_iv_bars(conn, symbol: str, limit: int = 252) -> list[IVBar]:
    '''
    Fetch the most recent `limit` IV bars, oldest first.
    Used by the batch job to compute iv_52w_high and iv_52w_low.
    Returns empty list if ticker not found.
    '''
    ticker_id = get_ticker_id(conn, symbol)
    if ticker_id is None:
        return []

    sql = '''
        SELECT date, close_iv
        FROM iv_bars
        WHERE ticker_id = %s
        ORDER BY date DESC
        LIMIT %s;
    '''

    with conn.cursor() as cur:
        cur.execute(sql, (ticker_id, limit))
        rows = cur.fetchall()

    return [
        IVBar(date=row[0], close_iv=float(row[1]))
        for row in reversed(rows)
    ]


def fetch_hv_values(conn, symbol: str) -> Optional[HVValues]:
    '''
    Fetch the stored HV and 52-week IV bounds for a ticker.
    Called during the trading day scan to get the pre-computed
    values needed for real-time IV rank calculation.

    Returns None if no values have been calculated yet.
    '''
    ticker_id = get_ticker_id(conn, symbol)
    if ticker_id is None:
        return None

    sql = '''
        SELECT hv_60, hv_252, iv_52w_high, iv_52w_low
        FROM hv_values
        WHERE ticker_id = %s;
    '''

    with conn.cursor() as cur:
        cur.execute(sql, (ticker_id,))
        row = cur.fetchone()

    if row is None:
        return None

    return HVValues(
        hv_60       = float(row[0]) if row[0] else None,
        hv_252      = float(row[1]) if row[1] else None,
        iv_52w_high = float(row[2]) if row[2] else None,
        iv_52w_low  = float(row[3]) if row[3] else None
    )


# =============================================================
# Real-time IV rank calculation (called during trading hours)
# =============================================================

def calculate_iv_rank(live_iv: float, hv: HVValues) -> Optional[float]:
    '''
    Calculate IV rank in real time using live IV and stored 52-week bounds.
    Call this during the trading day scan after fetching live IV from IBKR.

    Args:
        live_iv: current IV of the underlying from tickGeneric tickType 24
        hv     : HVValues fetched from DB (contains iv_52w_high, iv_52w_low)

    Returns:
        IV rank as 0-100 float, or None if bounds are missing or equal
    '''
    if hv.iv_52w_high is None or hv.iv_52w_low is None:
        return None

    iv_range = hv.iv_52w_high - hv.iv_52w_low
    if iv_range == 0:
        return 0.0

    return round((live_iv - hv.iv_52w_low) / iv_range * 100, 2)


# =============================================================
# Pruning
# =============================================================

def prune_bars(conn, symbol: str, keep: int = 252) -> tuple[int, int]:
    '''
    Delete price and IV bars beyond the most recent `keep` rows.
    Run at the end of each batch job to prevent unbounded growth.

    Returns:
        (price_bars_deleted, iv_bars_deleted)
    '''
    ticker_id = get_ticker_id(conn, symbol)
    if ticker_id is None:
        return (0, 0)

    price_sql = '''
        DELETE FROM price_bars
        WHERE ticker_id = %s
          AND date NOT IN (
              SELECT date FROM price_bars
              WHERE ticker_id = %s
              ORDER BY date DESC
              LIMIT %s
          );
    '''

    iv_sql = '''
        DELETE FROM iv_bars
        WHERE ticker_id = %s
          AND date NOT IN (
              SELECT date FROM iv_bars
              WHERE ticker_id = %s
              ORDER BY date DESC
              LIMIT %s
          );
    '''

    with conn.cursor() as cur:
        cur.execute(price_sql, (ticker_id, ticker_id, keep))
        price_deleted = cur.rowcount
        cur.execute(iv_sql, (ticker_id, ticker_id, keep))
        iv_deleted = cur.rowcount

    conn.commit()
    return (price_deleted, iv_deleted)


def prune_all_tickers(conn, keep: int = 252) -> dict[str, tuple[int, int]]:
    '''
    Run prune_bars() for every tracked ticker.
    Call once at the end of the daily batch job.

    Returns:
        dict mapping symbol -> (price_bars_deleted, iv_bars_deleted)
    '''
    with conn.cursor() as cur:
        cur.execute('SELECT symbol FROM tickers ORDER BY symbol')
        symbols = [row[0] for row in cur.fetchall()]

    results = {}
    for symbol in symbols:
        deleted = prune_bars(conn, symbol, keep)
        results[symbol] = deleted
        if any(deleted):
            print(f"[{symbol}] pruned — price: {deleted[0]}, iv: {deleted[1]}")

    return results