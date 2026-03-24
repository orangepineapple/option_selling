from datetime import datetime
from threading import Thread
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.common import BarData, TickerId
from trading_util.alert_util import PushNotification
from threading import Lock

from trading_util.data_util import Bar, Backfill

import time
import logging

logger = logging.getLogger(__name__)



class HistoricalData(EWrapper, EClient):
    '''
    Used to request any historical stock data from IB
    '''
    def __init__(self, addr, port, client_id):
        EWrapper.__init__(self)
        EClient. __init__(self, self)
        # Connect to TWS
        self.connect(addr, port, client_id)

        self.lock = Lock()

        self.data_arrived = []
        self.backfills = []

        self.return_data : dict[str,list[Bar]] = {} 

        self.invalid_tickers = []
        self.failed_to_connect = False

        self.pn = PushNotification("HISTORICAL DATA")

        # Launch the client thread
        thread = Thread(target=self.run)
        thread.start()
        time.sleep(0.5) # give this thread some time to start

    def _reset_counters(self):
        self.return_data = {}

    
    def get_historical_data(self, backfills : list[Backfill], bar_duration : str , type : str) -> dict[str, list[Bar]]:
        if len(backfills) == 0:
            return {}

        if self.failed_to_connect:
            return {}

        self._reset_counters()

        ### INIT OBJECT PROPERTIES ###
        DATA_TYPE = "TRADES"
        if type == "options":
            DATA_TYPE = "OPTION_IMPLIED_VOLATILITY"

        self.data_arrived = [False for x in range(len(backfills))]
        self.backfills = backfills

        #Request todays open and yesterdays close
        for index, backfill in enumerate(backfills) :
            stock = Contract()
            stock.symbol = backfill.ticker
            stock.secType = 'STK'
            stock.exchange = 'SMART'
            stock.currency = 'USD'    

            # Init the return data
            with self.lock:
                self.return_data[stock.symbol] = []  

            #req historical data volume TRADES
            self.reqHistoricalData(index , stock, '', backfill.delta, bar_duration , DATA_TYPE , 1, 1 , False, [])

        while self.check_all_data_arrived() == False:
            print("WAITING")
            time.sleep(1)
        
        # Remove Tickers that were not found
        for ticker in self.invalid_tickers:
            self.return_data.pop(ticker)
        
        print("HISTORICAL FINISHED", DATA_TYPE)

        return self.return_data
       

    def check_all_data_arrived(self):
        # check the the client is connected
        with self.lock:
            for item in self.data_arrived:
                if not item:
                    return False
        return True

    def generate_error_message(self) -> str:
        if len(self.invalid_tickers) == 0:
            return ""
        msg = "Not Found: "
        for ticker in self.invalid_tickers:
            msg += ticker + " "

        return msg

    def historicalData(self, reqId: int, bar: BarData):
        with self.lock:
            self.return_data[self.backfills[reqId].ticker].append(
                Bar( 
                    date = datetime.strptime(bar.date, "%Y%m%d"),
                    open = bar.open, 
                    close = bar.close, 
                    high = bar.high, 
                    low = bar.low, 
                    volume = bar.volume, 
                    num_trades = bar.barCount
                )   
                
            )

    def historicalDataEnd(self, reqId, start, end):
        # This will wait for all historical Data to run before firing
        with self.lock:
            self.data_arrived[reqId] = True

    def error(self, reqId: int, errorTime: int, errorCode: int, errorString: str, advancedOrderRejectJson=""):
        if errorCode == 200: # Failed request (likely unrecognized ticker in our case)
            print(self.backfills[reqId].ticker, errorString)
            with self.lock:
                self.data_arrived[reqId] = True
                self.invalid_tickers.append(self.backfills[reqId].ticker)
        if errorCode == 504: # client not connected error
            if not self.failed_to_connect:
                self.pn.send_notif("@everyone gateway is not connected, please restart manually")    
            self.failed_to_connect = True
        else:
            print(errorCode, errorString)