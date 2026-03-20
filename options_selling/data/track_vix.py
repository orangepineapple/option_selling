#We want to get a better understanding of IV - and IV skew
#plot the volatility surface in plotly of a graph
from datetime import datetime
from threading import Thread
from unicodedata import name
from ibapi.client import EClient
from ibapi.common import TickerId
from ibapi.ticktype import TickType
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import time


class VIXData(EWrapper, EClient):
    ''' Serves as the client and the wrapper '''
    def __init__(self, addr, port, client_id):
        EWrapper.__init__(self)
        EClient. __init__(self, self)
        # Connect to TWS
        self.connect(addr, port, client_id)

        self.vix_price = None

        self.waiting_for_vix = True

        self.vix_historical = {}
        self.waiting_for_vix_historical = True
        # Launch the client thread
        thread = Thread(target=self.run)
        thread.start()
        time.sleep(0.5)#give this thread some time to start
    
    def get_vix(self):
        '''
        Gets the options implied volatility as well as all the greeks for the selected options
        today: date string in the form yyyymmdd
        '''
        vix = Contract()
        vix.symbol = "VIX"
        vix.secType = "IND"
        vix.exchange = "CBOE"
        vix.currency = "USD"

        self.reqMktData(1, vix, "", False, False, [])


        while self.waiting_for_vix:
            time.sleep(1)
        
        self.cancelMktData(1)
        return self.vix_price


    def vix_historical_backfill(self):
        '''
        backfills day by day historical data for the vix to see average vix ranges used to identify if wix is high low or medium
        '''
        vix = Contract()
        vix.symbol = "VIX"
        vix.secType = "IND"
        vix.exchange = "CBOE"
        vix.currency = "USD"

        self.reqHistoricalData(1, vix , "", "5 Y", "1 day", "TRADES", 0, 1, False, [] )

        while self.waiting_for_vix_historical:
            time.sleep(1)

        return self.vix_historical

    #CALLBACK for reqMktData
    def tickPrice(self, reqId, tickType, price, attrib):
        if tickType == 4: #last price
            self.vix_price = price
            self.waiting_for_vix = False


    def historicalData(self, reqId, bar):
        self.vix_historical[bar.date] = {"open" : bar.open, "close" : bar.close}    

    def historicalDataEnd(self, reqId, start, end):
        self.waiting_for_vix_historical = False

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        print(errorCode, errorString)
        if errorCode != 2104 and errorCode != 2106 and errorCode != 2158:
            print("REAL ERROR" , errorCode, errorString)
            time.sleep(1)
            self.disconnect()

def main():
    client = VIXData('127.0.0.1', 4002, 0)
    client.get_vix()
    print(client.vix_price)
    #print(client.vix_historical_backfill())

    client.disconnect()



if __name__ == '__main__':
    main()