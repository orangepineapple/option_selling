from data.IB_historical import HistoricalData
from config.constants import HOST, CLIENT_NUM
from data.db_operations import upsert_price_bars, upsert_iv_bars, fetch_price_bars, get_universe , upsert_hv_values, fetch_iv_bars
from data.db_connect import connect, close_connection
from trading_util.data_util import Backfill, HVValues
from trading_util.date_util import isTradingDay
from data.calculate_HV import yang_zhang_volatility
from data.iv_ranking import calculate_iv_percentile

def get_backfill_plans(conn, tickers) -> tuple[list[Backfill], list[Backfill]]:
    # The query template for both tables
    # We use %s::varchar[] to tell Postgres this is an array of strings
    query = """
        SELECT t.ticker_col, (CURRENT_DATE - MAX(table_alias.date)) as gap
        FROM (SELECT UNNEST(%s::varchar[]) as ticker_col) t
        LEFT JOIN {table} table_alias ON table_alias.{col} = t.ticker_col
        GROUP BY t.ticker_col
    """

    iv_backfills = []
    price_backfills = []

    with conn.cursor() as cur:
        # 1. Fetch all IV gaps in one trip
        cur.execute(query.format(table="iv_bars", col="symbol"), (tickers,))
        for ticker, gap in cur.fetchall():
            # If gap is 0 or less, they are up to date, so we skip
            if gap is None or gap >= 1:
                iv_backfills.append(Backfill(ticker=ticker, delta=ibkr_duration_string(gap)))

        # 2. Fetch all Price gaps in one trip
        cur.execute(query.format(table="daily_historical", col="stock"), (tickers,))
        for ticker, gap in cur.fetchall():
            if gap is None or gap >= 1:
                price_backfills.append(Backfill(ticker=ticker, delta=ibkr_duration_string(gap)))

    return iv_backfills, price_backfills

def ibkr_duration_string(days_missing: int) -> str:
    # None means the LEFT JOIN found no data for this ticker
    if days_missing is None:
        return "252 D"

    if days_missing > 364:
        return "3 Y"
    
    return f"{days_missing} D"


def caluclate_historical_IV():
    if not isTradingDay(): return
    conn = connect()
    universe = get_universe(conn)

    print("UNIVERSE", universe)

    ## UPDATE THE DATA ##
    iv_backfills, price_backfills = get_backfill_plans(conn, universe)

    print("BACKFILLS", price_backfills, iv_backfills)
    client = HistoricalData(HOST, 4002, CLIENT_NUM)
    print("CALL PRICE DATA")
    price_data = client.get_historical_data(price_backfills, "1 day", "stock")
    print("CALL IVS")
    iv_data = client.get_historical_data(iv_backfills, "1 day", "options")

    client.disconnect()

    print("PRICE DATA", len(price_data))
    print("IV_DATA", len(iv_data))

    ## SAVE THE UPDATES BACK INTO THE DB ##
    for symbol, bars in price_data.items():
        upsert_price_bars(conn, symbol, bars)

    for symbol, bars in iv_data.items():
        upsert_iv_bars(conn, symbol, bars)
    
    ## PULL DATA AND DO CALCULATIONS

    for ticker in universe:    
        one_year_historical = fetch_price_bars(conn, ticker)

        # 60-day window — use the most recent 60 bars
        hv_60 = None
        if len(one_year_historical) >= 60:
            hv_60 = yang_zhang_volatility(one_year_historical[-60:])
        else:
            print(f"[{ticker}] insufficient bars for hv_60: {len(one_year_historical)} < 60")

        # 252-day window — use the most recent 252 bars
        hv_252 = None
        if len(one_year_historical) >= 252:
            hv_252 = yang_zhang_volatility(one_year_historical[-252:])
        else:
            print(f"[{ticker}] insufficient bars for hv_252: {len(one_year_historical)} < 252")

        curr_iv = fetch_iv_bars(conn, ticker, 1)[0].close_iv
        perc = calculate_iv_percentile(conn, ticker, curr_iv)
        

        hv = HVValues(
            ticker     = ticker,
            hv_60      = hv_60,
            hv_252     = hv_252,
            iv_percent = perc
        )

        print(hv)

        upsert_hv_values(conn, ticker, hv)

    close_connection(conn)


