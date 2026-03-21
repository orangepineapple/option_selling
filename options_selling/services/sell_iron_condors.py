from data.options_chain import OptionsChainPrices
from data.db_connect import connect, close_connection
from trading_util import isTradingDay
from config.constants import HOST, CLIENT_NUM
from datetime import datetime, timedelta

def scan_and_enter_positions():
    if not isTradingDay(): return

    client = OptionsChainPrices(HOST, 4002, CLIENT_NUM)
    chain , high_opt, low_opt = client.get_options_chain("AAPL", datetime.today() + timedelta(days=45))

    
    return
