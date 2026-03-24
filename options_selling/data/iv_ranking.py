def calculate_iv_percentile(conn, symbol: str, live_iv: float) -> float:
    '''
    What percentage of days in the past 252 had IV lower than today.
    More robust than IV rank when there are outlier spikes.
    '''
    with conn.cursor() as cur:
        cur.execute('''
            SELECT COUNT(*) FILTER (WHERE close_iv < %s) * 100.0 / COUNT(*)
            FROM (
                SELECT close_iv FROM iv_bars
                WHERE symbol = %s
                ORDER BY date DESC
                LIMIT 252
            ) recent;
        ''', (live_iv, symbol.upper()))
        return round(cur.fetchone()[0], 2)