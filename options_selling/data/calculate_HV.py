import math
from typing import Optional
from trading_util.data_util import Bar

def yang_zhang_volatility(bars: list[Bar]) -> Optional[float]:
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