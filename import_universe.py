from options_selling.data.db_operations import add_ticker, fetch_iv_bars
from options_selling.data.db_connect import close_connection, connect
from options_selling.data.iv_ranking import calculate_iv_percentile

conn = connect()


perc = calculate_iv_percentile(conn, "SPY", 0.2125)
print(perc)
close_connection(conn)
