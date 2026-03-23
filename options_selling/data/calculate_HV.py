import math
from datetime import datetime
from dataclasses import dataclass


@dataclass
class Bar:
    date      : datetime
    open      : float
    close     : float
    high      : float
    low       : float
    volume    : float
    num_trades: int


@dataclass
class HistoricalVolatility:
    ticker    : str
    hv_60     : float | None   # 60-day  Yang-Zhang HV, annualized
    hv_252    : float | None   # 252-day Yang-Zhang HV, annualized
    calculated: datetime       # timestamp of when this was computed


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


def calculate_historical_volatility(
    data: dict[str, list[Bar]]
) -> dict[str, HistoricalVolatility]:
    '''
    Calculate 60-day and 252-day Yang-Zhang historical volatility
    for each security in the input data.

    Args:
        data: dict mapping ticker -> list[Bar], sorted oldest to newest.
              Bars should be daily. 1 year of data is sufficient for
              both windows. For delta updates, pass only the new bars
              merged with enough history to fill the longest window (252).

    Returns:
        dict mapping ticker -> HistoricalVolatility with hv_60 and hv_252.
        If there is insufficient data for a window, that field is None.

    Usage — new security (no DB record):
        Call 1 year of bars, pass directly.

    Usage — existing security (delta update):
        Call X days of new bars, merge with last 252 bars from DB,
        sort by date, pass in. Only the latest HV values need to be stored.
    '''
    results: dict[str, HistoricalVolatility] = {}
    now = datetime.utcnow()

    for ticker, bars in data.items():

        if not bars:
            results[ticker] = HistoricalVolatility(
                ticker     = ticker,
                hv_60      = None,
                hv_252     = None,
                calculated = now
            )
            continue

        # Sort oldest to newest — Yang-Zhang requires chronological order
        sorted_bars = sorted(bars, key=lambda b: b.date)

        # 60-day window — use the most recent 60 bars
        hv_60 = None
        if len(sorted_bars) >= 60:
            hv_60 = _yang_zhang(sorted_bars[-60:])
        else:
            print(f"[{ticker}] insufficient bars for hv_60: {len(sorted_bars)} < 60")

        # 252-day window — use the most recent 252 bars
        hv_252 = None
        if len(sorted_bars) >= 252:
            hv_252 = _yang_zhang(sorted_bars[-252:])
        else:
            print(f"[{ticker}] insufficient bars for hv_252: {len(sorted_bars)} < 252")

        results[ticker] = HistoricalVolatility(
            ticker     = ticker,
            hv_60      = hv_60,
            hv_252     = hv_252,
            calculated = now
        )

    return results


def merge_bars_for_delta_update(
    existing_bars : list[Bar],
    new_bars      : list[Bar],
    window        : int = 252
) -> list[Bar]:
    '''
    Merge existing bars from DB with newly fetched bars for a delta update.
    Deduplicates by date and returns the most recent `window` bars sorted
    oldest to newest — just enough history to calculate all HV windows.

    Args:
        existing_bars: bars already stored in DB for this ticker
        new_bars     : freshly fetched bars from IBKR
        window       : how many bars to retain (default 252 — longest window)

    Returns:
        Merged, deduplicated, sorted list trimmed to `window` bars.
    '''
    # Deduplicate by date — new bars win on conflict
    bar_map: dict[datetime, Bar] = {b.date: b for b in existing_bars}
    bar_map.update({b.date: b for b in new_bars})

    merged = sorted(bar_map.values(), key=lambda b: b.date)

    # Only keep enough history for the longest window
    return merged[-window:]