from data.IB_historical import HistoricalData
from config.constants import HOST, CLIENT_NUM
from data.db_operations import upsert_price_bars, upsert_iv_bars, fetch_price_bars, get_universe , upsert_hv_values
from data.db_connect import connect, close_connection
from trading_util.data_util import Bar, Backfill, HistoricalVolatility

import math

from datetime import  datetime


def caluclate_historical_IV():
    # if not isTradingDay(): return
    conn = connect()

    ## UPDATE THE DATA ##
    price_backfills, iv_backfills = get_backfill_status(conn)
    client = HistoricalData(HOST, 4002, CLIENT_NUM)
    price_data = client.get_historical_data(price_backfills, "1 day", "stock")
    iv_data = client.get_historical_data(iv_backfills, "1 day", "options")

    ## SAVE THE UPDATES BACK INTO THE DB ##
    for symbol, bars in price_data.items():
        upsert_price_bars(conn, symbol, bars)

    for symbol, bars in iv_data.items():
        upsert_iv_bars(conn, symbol, bars)
    

    ## PULL DATA AND DO CALCULATIONS
    universe = get_universe(conn)
    for ticker in universe:    
        one_year_historical = fetch_price_bars(conn, ticker)

        # 60-day window — use the most recent 60 bars
        hv_60 = None
        if len(one_year_historical) >= 60:
            hv_60 = _yang_zhang(one_year_historical[-60:])
        else:
            print(f"[{ticker}] insufficient bars for hv_60: {len(one_year_historical)} < 60")

        # 252-day window — use the most recent 252 bars
        hv_252 = None
        if len(one_year_historical) >= 252:
            hv_252 = _yang_zhang(one_year_historical[-252:])
        else:
            print(f"[{ticker}] insufficient bars for hv_252: {len(one_year_historical)} < 252")

        hv = HistoricalVolatility(
            ticker     = ticker,
            hv_60      = hv_60,
            hv_252     = hv_252,
            calculated = datetime.now()
        )

        upsert_hv_values(conn, ticker, hv)

    close_connection(conn)


def get_backfill_status(conn) -> tuple[list[Backfill], list[Backfill]]:
    '''
    Returns one BackfillStatus per ticker in the tickers table.
    Checks price_bars and iv_bars independently so each table
    can be backfilled separately if needed.
    '''
    sql = '''
        SELECT
            t.symbol,
            MAX(pb.date)                                AS last_price_bar,
            MAX(ib.date)                                AS last_iv_bar,
            CASE
                WHEN MAX(pb.date) IS NULL THEN -1
                ELSE (CURRENT_DATE - MAX(pb.date))::INT
            END                                         AS price_days_missing,
            CASE
                WHEN MAX(ib.date) IS NULL THEN -1
                ELSE (CURRENT_DATE - MAX(ib.date))::INT
            END                                         AS iv_days_missing,
            MAX(pb.date) IS NULL                        AS needs_price_backfill,
            MAX(ib.date) IS NULL                        AS needs_iv_backfill

        FROM tickers t
        LEFT JOIN price_bars pb ON pb.ticker_id = t.id
        LEFT JOIN iv_bars    ib ON ib.ticker_id = t.id

        GROUP BY t.id, t.symbol
        ORDER BY t.symbol;
    '''

    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    iv_backfills = []
    price_backfills = []

    # BackfillStatus(
    #         symbol               = row[0],
    #         last_price_bar       = row[1],
    #         last_iv_bar          = row[2],
    #         price_days_missing   = row[3],
    #         iv_days_missing      = row[4],
    #         needs_price_backfill = row[5],
    #         needs_iv_backfill    = row[6]
    #     )

    for row in rows:
        if row[5]:
            price_backfills.append(Backfill(
                ticker=row[0], 
                delta=ibkr_duration_string(row[3])
            ))
        if row[6]:
            iv_backfills.append(
                Backfill(
                    ticker=row[0],
                    delta=ibkr_duration_string(row[4])
                )
            )

    return price_backfills, iv_backfills


def ibkr_duration_string(days_missing: int, full_backfill_duration: str = "1 Y") -> str:

    if days_missing == -1:
        return full_backfill_duration

    if days_missing > 364:
        return "3 Y"
    else:
        return str(days_missing) + " D"


def _yang_zhang(bars: list[Bar]) -> float:
    '''
    Yang-Zhang volatility estimator.

    Uses overnight gaps (close-to-open) and intraday range (high-low)
    to produce a more accurate volatility estimate than close-to-close.

    Returns annualized volatility as a decimal (e.g. 0.25 = 25%).

    Formula components:
      - overnight variance (close-to-open moves)
      - open-to-close variance (Rogers-Satchell component)
      - k: weighting factor that minimizes estimator variance
    '''
    n = len(bars)
    if n < 2:
        return None

    # Pre-compute log returns needed for each component
    # Rogers-Satchell handles intraday drift-independent variance
    rs_sum        = 0.0   # Rogers-Satchell sum
    overnight_sum = 0.0   # sum of overnight log returns
    oc_sum        = 0.0   # sum of open-to-close log returns

    overnight_sq_sum = 0.0
    oc_sq_sum        = 0.0

    # We need pairs of bars for overnight gaps so start at index 1
    for i in range(1, n):
        prev  = bars[i - 1]
        curr  = bars[i]

        log_ho = math.log(curr.high  / curr.open)
        log_lo = math.log(curr.low   / curr.open)
        log_co = math.log(curr.close / curr.open)
        log_oc = math.log(curr.open  / prev.close)   # overnight gap

        # Rogers-Satchell: drift-independent intraday variance
        rs = log_ho * (log_ho - log_co) + log_lo * (log_lo - log_co)
        rs_sum += rs

        overnight_sum    += log_oc
        overnight_sq_sum += log_oc ** 2

        oc_sum    += log_co
        oc_sq_sum += log_co ** 2

    # Number of valid pairs
    m = n - 1

    # Overnight variance
    overnight_mean = overnight_sum / m
    overnight_var  = (overnight_sq_sum / m) - (overnight_mean ** 2)

    # Open-to-close variance
    oc_mean = oc_sum / m
    oc_var  = (oc_sq_sum / m) - (oc_mean ** 2)

    # Rogers-Satchell variance (already mean-corrected by construction)
    rs_var = rs_sum / m

    # Yang-Zhang weighting factor k (minimizes estimator variance)
    k = 0.34 / (1.34 + (m + 1) / (m - 1))

    # Combined Yang-Zhang variance
    yz_var = overnight_var + k * oc_var + (1 - k) * rs_var

    # Annualize: multiply by 252 trading days then take sqrt
    annualized_vol = math.sqrt(max(yz_var, 0) * 252)

    return round(annualized_vol, 6)
