import psycopg2
import psycopg2.extras
from trading_util.data_util import Bar, HVValues, IVBar
from typing import Optional

# =============================================================
# Ticker operations
# =============================================================

def get_universe(conn) -> list[str]:
    '''
    Return all tracked ticker symbols from the tickers registry.
    Source of truth for the universe — used by all services.
    '''
    with conn.cursor() as cur:
        cur.execute('SELECT symbol FROM options_universe ORDER BY symbol;')
        rows = cur.fetchall()
    return [row[0] for row in rows]
 
 
def ticker_exists(conn, symbol: str) -> bool:
    ''' Return True if the ticker is in the registry '''
    with conn.cursor() as cur:
        cur.execute(
            'SELECT 1 FROM options_universe WHERE symbol = %s;',
            (symbol.upper(),)
        )
        return cur.fetchone() is not None
 
 
def add_ticker(conn, symbol: str) -> bool:
    '''
    Add a ticker to the registry.
    Returns True if inserted, False if it already existed.
    '''
    with conn.cursor() as cur:
        cur.execute(
            '''
            INSERT INTO options_universe (symbol)
            VALUES (%s)
            ON CONFLICT (symbol) DO NOTHING
            RETURNING symbol;
            ''',
            (symbol.upper(),)
        )
        inserted = cur.fetchone()
    conn.commit()
    return inserted is not None
 
 
def remove_ticker(conn, symbol: str) -> bool:
    '''
    Remove a ticker from the registry and delete all associated
    data from every table. No cascades — each table is deleted
    from explicitly.
    Returns True if the ticker existed, False if not found.
    '''
    symbol = symbol.upper()
    with conn.cursor() as cur:
        cur.execute(
            'DELETE FROM options_universe WHERE symbol = %s RETURNING symbol;',
            (symbol,)
        )
        deleted = cur.fetchone()
 
        if deleted is not None:
            cur.execute('DELETE FROM daily_historical WHERE stock = %s;',  (symbol,))
            cur.execute('DELETE FROM iv_bars WHERE symbol = %s;',          (symbol,))
            cur.execute('DELETE FROM hv_values WHERE symbol = %s;',        (symbol,))
 
    conn.commit()
    return deleted is not None

# =============================================================
# Price bar operations
# =============================================================

def upsert_price_bars(conn, symbol: str, bars: list[Bar]) -> int:
    sql = '''
        INSERT INTO daily_historical
            (date, stock, open, close, high, low, volume, num_trade)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date, stock) DO UPDATE SET
            open      = EXCLUDED.open,
            close     = EXCLUDED.close,
            high      = EXCLUDED.high,
            low       = EXCLUDED.low,
            volume    = EXCLUDED.volume,
            num_trade = EXCLUDED.num_trade;
    '''

    rows = [
        (b.date, symbol.upper(), b.open, b.close, b.high, b.low, b.volume, b.num_trades)
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
    Upsert IV bars for a ticker, storing only the close IV.
    Accepts standard Bar objects — only bar.close is stored.
 
    Returns:
        number of rows affected
    '''
    sql = '''
        INSERT INTO iv_bars (symbol, date, close_iv)
        VALUES (%s, %s, %s)
        ON CONFLICT (date, symbol) DO UPDATE SET
            close_iv = EXCLUDED.close_iv;
    '''
 
    rows = [(symbol.upper(), b.date, b.close) for b in bars]
 
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, rows)
        count = cur.rowcount
    conn.commit()
    return count
 
 
def fetch_iv_bars(conn, symbol: str, limit: int = 252) -> list[IVBar]:
    sql = '''
        SELECT date, close_iv
        FROM iv_bars
        WHERE symbol = %s
        ORDER BY date DESC
        LIMIT %s;
    '''

    with conn.cursor() as cur:
        cur.execute(sql, (symbol.upper(), limit))
        rows = cur.fetchall()

    return [
        IVBar(date=row[0], close_iv=float(row[1]))
        for row in reversed(rows)
    ]

# =============================================================
# HV values operations
# =============================================================

def upsert_hv_values(conn, symbol: str, hv: HVValues) -> None:
    sql = '''
        INSERT INTO hv_values
            (symbol, hv_60, hv_252, iv_percentile, calculated_at)
        VALUES (%s, %s, %s, %s, NOW())
        ON CONFLICT (symbol) DO UPDATE SET
            hv_60         = EXCLUDED.hv_60,
            hv_252        = EXCLUDED.hv_252,
            iv_percentile = EXCLUDED.iv_percentile,
            calculated_at = NOW();
    '''

    with conn.cursor() as cur:
        cur.execute(sql, (
            symbol.upper(),
            hv.hv_60,
            hv.hv_252,
            hv.iv_percent,
        ))
    conn.commit()

def fetch_hv_values(conn, symbol: str) -> Optional[HVValues]:
    sql = '''
        SELECT hv_60, hv_252, iv_percentile
        FROM hv_values
        WHERE symbol = %s;
    '''

    with conn.cursor() as cur:
        cur.execute(sql, (symbol.upper(),))
        row = cur.fetchone()

    if row is None:
        return None

    return HVValues(
        ticker        = symbol.upper(),
        hv_60         = float(row[0]) if row[0] else None,
        hv_252        = float(row[1]) if row[1] else None,
        iv_percent = float(row[2]) if row[2] else None,
    )


# =============================================================
# Fetch bars for recalculation
# =============================================================

def fetch_price_bars(conn, symbol: str, limit: int = 252) -> list[Bar]:
    sql = '''
        SELECT date, open, high, low, close, volume, num_trade
        FROM daily_historical
        WHERE stock = %s
        ORDER BY date DESC
        LIMIT %s;
    '''

    with conn.cursor() as cur:
        cur.execute(sql, (symbol.upper(), limit))
        rows = cur.fetchall()

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