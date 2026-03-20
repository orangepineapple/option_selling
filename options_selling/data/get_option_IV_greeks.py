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

class OptionsGetImpliedVol(EWrapper, EClient):
    ''' Serves as the client and the wrapper '''
    def __init__(self, addr, port, client_id):
        EWrapper.__init__(self)
        EClient. __init__(self, self)
        # Connect to TWS
        self.connect(addr, port, client_id)

        self.got_implied_vol = None
        self.got_30_day_historical_vol = None
        self.done_getting_greeks = None
        self.last_req = None

        self.req_id = 1

        self.return_vol = {}

        self.ticker_id_map = {}

        self.volatility_req_tracker = {}

        self.remaining_requests = None


        # Launch the client thread
        thread = Thread(target=self.run)
        thread.start()
        time.sleep(0.5)#give this thread some time to start
    
    def get_option_greeks_and_IV(self, ticker_list):
        '''
        Gets the options implied volatility as well as all the greeks for the selected options
        today: date string in the form yyyymmdd
        '''

        self.remaining_requests = len(ticker_list)

        for ticker in ticker_list:
            underlying_contract = Contract()
            underlying_contract.symbol = ticker
            underlying_contract.secType = 'STK'
            underlying_contract.exchange = 'SMART'
            underlying_contract.currency = 'USD'

            self.reqMktData(self.req_id, underlying_contract, '106,104', False, False, [])
            self.ticker_id_map[self.req_id] = ticker
            self.volatility_req_tracker[self.req_id] = 2
            self.req_id += 1

        while self.remaining_requests > 0:
            time.sleep(1)

        
        

        print(stock_price)

        self.last_req = len(option_prices)

        for row in option_prices:
            expiry = row[0].strftime('%Y%m%d') 
            right = row[1]
            strike = row[2]
            last_price = row[3]
            self.req_id += 1

            print("expiry : " + expiry + " right : " + str(right) + " stirke : " + str(strike) + " last_price : " + str(last_price))

            contract = Contract()
            contract.symbol = ticker
            contract.secType = "OPT"
            contract.exchange = "SMART"
            contract.currency = "USD"
            contract.lastTradeDateOrContractMonth = expiry
            contract.strike = strike
            contract.right = right
            contract.multiplier = "100"


            self.calculateImpliedVolatility(self.req_id, contract, last_price, stock_price, [])
        
        while self.done_getting_greeks is None:
            time.sleep(1)
        
        return self.return_vol
    
    #CALLBACK for 
    def tickOptionComputation(self, reqId, tickType, tickAttrib, impliedVol, delta, optPrice, pvDividend, gamma, vega, theta, undPrice):
        print("implied vol: ", impliedVol)
        print("delta: ", delta)
        print("options price: ",optPrice)
        print("Dividends: ",pvDividend)
        print("gamma: ",gamma)
        print("vega: ", vega)
        print("theta: ",theta)
        print("underlying price: ", undPrice)

        if reqId == self.last_req:
            self.done_getting_greeks = True

    #CALLBACK for reqMktData
    def tickGeneric(self, reqId: TickerId, tickType: TickType, value: float):
        print(tickType)
        print(value)

        if tickType == 24: 
            self.return_vol[reqId]["implied_vol"] = value
        
        if tickType == 23:
            self.return_vol[reqId]["30_day_historical_vol"] = value
        
        if self.volatility_req_tracker[reqId] == 0:
            self.cancelMktData(reqId)
            self.remaining_requests -= 1
    



#TODO
# There are 2 ways to calculate the implied volatility we will do all the following
# 1) Get the implied volatility of the underlying security from reqMktData()
# 2) use calculateImpliedVolatility to calculate it based on the either the bid/ask or last price       


def main():
    client = OptionsGetImpliedVol('127.0.0.1', 4002, 0)
    client.get_underlying_implied_vol("TSLA")



    today = datetime.today().strftime('%Y-%m-%d')
    print(today)
    
    client.get_options_chain_implied_vol("TSLA", today)

    client.disconnect()



if __name__ == '__main__':
    main()