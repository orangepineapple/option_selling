from datetime import datetime, timedelta
from threading import Thread
from unicodedata import name
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import time

class Option():
    def __init__(self, expiry, strike, right):
        self.expiry = expiry
        self.strike = strike
        self.right = right
    
    def __str__(self):
        return f"Expiry: {self.expiry} , Strike: {self.strike} , Right: {self.right}"

class OptionsChainPrices(EWrapper, EClient):
    ''' Serves as the client and the wrapper '''
    def __init__(self, addr, port, client_id):
        EWrapper.__init__(self)
        EClient. __init__(self, self)
        # Connect to TWS
        self.connect(addr, port, client_id)

        self.ticker = None
        self.exchange = None
        self.expirations = None
        self.strikes = None

        self.underlying_price = None
        self.options_chain_returned = None

        self.chain = set()

        self.req_id = 2

        self.options_price_chain = {}
        self.option_request_track = {}

        self.target_expiry = None
        self.target_delta = None

        self.num_options_contracts_remaining = None

        self.closest_low_date = datetime.today() - timedelta(days=365)
        self.closest_high_date = datetime.today() + timedelta(days=365)

        # Launch the client thread
        thread = Thread(target=self.run)
        thread.start()
        time.sleep(0.5)#give this thread some time to start


    def get_options_chain(self, ticker : str, target_expiry_date : datetime ):
        '''
        External entry function to get the options chain:
        Fetches the prices for options, with the input delta, and inputed expiry date
        '''
        self.ticker = ticker
        self.target_expiry = target_expiry_date

        print("target expiry:" , self.target_expiry)

        #Request the last price of the underlying
        underlying_contract = Contract()
        underlying_contract.symbol = ticker
        underlying_contract.secType = 'STK'
        underlying_contract.exchange = 'SMART'
        underlying_contract.currency = 'USD'
        self.reqMktData(0, underlying_contract, '', True, False, [])


        #Request the options chain: will request all possible strikes and expiries
        options_contract = Contract()
        options_contract.symbol = ticker
        options_contract.secType = 'OPT'
        options_contract.currency = 'USD'
        options_contract.exchange = 'CBOE'

        self.reqContractDetails(1 , options_contract)

        #Everything after this point requres underlying price
        while self.underlying_price is None:
            time.sleep(1)
        print("underlying_price received")

        while self.options_chain_returned is None:
            time.sleep(1)
        print("Options Chain returned")

        ### Format Options Chain Nicer ###
        return_chain = []
        for option_hash in self.chain:
            data = option_hash.split("/")
            expiry, right, strike = data[0], data[1] , data[3]
            return_chain.append(Option(datetime.strptime(expiry, '%Y%m%d'), strike, right))
        
        return return_chain, self.closest_high_date, self.closest_low_date

    def request_option_prices_greeks(self, options_chain , ticker ):
        '''
        Provides options prices and greeks for specifiedf options chain. 
        options_chain: collection of hashes representing expiry date + right + strike
                expiry date: str '%Y%m%d'
                right: "P" or "C"
                strike: int
        
        ticker: str
        '''
        request_index_to_option_map = {} 

        self.num_options_contracts_remaining = len(options_chain)

        for option_hash in options_chain:
            data = option_hash.split("/")
            expiry, right, strike = data[0], data[1] , data[3]

            option = Contract()
            option.symbol = ticker
            option.secType = 'OPT'
            option.currency = 'USD'
            option.exchange = 'CBOE'
            option.lastTradeDateOrContractMonth = expiry
            option.strike = strike
            option.right = right
            
            self.reqMktData(self.req_id, option , '', True, False, [])
            self.option_request_track[self.req_id] = 3
            self.options_price_chain[self.req_id] = {} #init the price chain
            request_index_to_option_map[option_hash] = self.req_id
            self.req_id += 1
        
        while self.num_options_contracts_remaining > 0:
            print("contracts remaining", self.num_options_contracts_remaining)
            time.sleep(1)
        print("Done processing option greeks")
        

        ### Once the prices are retrieved, we can get the greeks and IV
        ### Map the hash back to the price chain
        self.num_options_contracts_remaining = len(self.options_price_chain)
        for id in self.options_price_chain:
            data = option_hash.split("/")
            expiry, right, strike = data[0], data[1] , data[3]
            last_price = self.options_price_chain[id]["last_price"]

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

            #im not sure if i can reuse id's but hopefully
            self.calculateImpliedVolatility(id, contract, last_price, self.underlying_price, [])
        
        while self.num_options_contracts_remaining > 0:
            time.sleep(1)
        
        return self.options_price_chain

    #CALLBACK - reqContractDetails
    def contractDetails(self, reqId, desc):
        '''
        when requesting options, every possible strike price is returned, this function gets returned to multiple times, which each possible value in the chain
        we can do some data processing here to try to get the expiry which is closest to our desired time, and load all the strikes for that
        '''

        if reqId == 1:
            strike = desc.contract.strike
            #print(desc.contract.exchange)
            #print(desc.contract.multiplier)
            right = desc.contract.right
            expiry_date =  datetime.strptime(desc.contract.lastTradeDateOrContractMonth, '%Y%m%d')
            expiry_hash = desc.contract.lastTradeDateOrContractMonth + "/" + right + "/" + str(strike)
            
            if expiry_hash not in self.chain:
                self.chain.add(expiry_hash)

            ### We can to find the dates that are closest to the target time ###
            if expiry_date >= self.target_expiry: 
                if expiry_date < self.closest_high_date:
                    self.closest_high_date = expiry_date
            else:
                if expiry_date > self.closest_low_date:
                    self.closest_low_date = expiry_date
            
            print("received", strike, right, expiry_date, expiry_hash)

    #CALLBACK
    def contractDetailsEnd(self, reqId):
        '''
        gets triggered when all the options contracts are done returning
        '''
        if reqId == 1:
            self.options_chain_returned = True
        

    #CALLBACK for 
    def tickOptionComputation(self, reqId, tickType, tickAttrib, impliedVol, delta, optPrice, pvDividend, gamma, vega, theta, undPrice):
        self.options_price_chain[reqId]["implied vol"] = impliedVol
        self.options_price_chain[reqId]["delta"] = delta
        self.options_price_chain[reqId]["options price"] = optPrice
        self.options_price_chain[reqId]["Dividends"] = pvDividend
        self.options_price_chain[reqId]["gamma"] = gamma
        self.options_price_chain[reqId]["vega"] = vega
        self.options_price_chain[reqId]["theta"] = theta

        self.num_options_contracts_remaining -= 1


    def tickPrice(self, req_id, field, price, attribs):
        ''' Provide option's ask price/bid price '''

        if req_id == 0: #we know its the underlying request
            if field == 4:
                self.underlying_price = price
            else:
                print('tickPrice - field: {}, price: {}'.format(field,price))
        else: 
            if field == 4: #LAST PRICE
                self.options_price_chain[req_id]['last_price'] = price
                self.option_request_track[req_id] -= 1
                if self.option_request_track[req_id] == 0:
                    self.num_options_contracts_remaining -= 1

            if field == 1: #BID PRICE
                self.options_price_chain[req_id]['bid_price'] = price
                self.option_request_track[req_id] -= 1
                if self.option_request_track[req_id] == 0:
                    self.num_options_contracts_remaining -= 1

            if field == 2: #ASK PRICE
                self.options_price_chain[req_id]['ask_price'] = price
                self.option_request_track[req_id] -= 1
                if self.option_request_track[req_id] == 0:
                    self.num_options_contracts_remaining -= 1
