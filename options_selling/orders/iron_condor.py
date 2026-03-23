from datetime import datetime
from threading import Thread
from ibapi.client import EClient
from ibapi.common import OrderId
from ibapi.order_state import OrderState
from ibapi.wrapper import EWrapper
from ibapi.utils import iswrapper
from ibapi.order import Order
from ibapi.contract import Contract , ComboLeg
import time

class IronCondor(EWrapper, EClient):
    ''' Serves as the client and the wrapper '''
    def __init__(self, addr, port, client_id):
        EWrapper.__init__(self)
        EClient. __init__(self, self)
        
        self.order_id = None
        self.contract = None
        self.order = None

        self.short_call_id = None
        self.short_put_id = None
        self.long_call_id = None
        self.long_put_id = None

        self.ticker = None
        self.expiry = None
        self.strike = None

        self.loop_id = None
        
        self.short_call_strike = None
        self.short_put_strike = None
        self.long_put_strike = None
        self.long_call_strike = None

        self.amount = None
        self.received_condor_data = False

        # Connect to TWS
        self.connect(addr, port, client_id)
        
        # Launch the client thread
        thread = Thread(target=self.run)
        thread.start()
        time.sleep(0.5)
    
    
    def get_condor_options_data(self, loop_id):
        '''
        A help function for the class 
        '''

        if loop_id == 0:
            #on loop one we get the call with strike price k1
            curr_right = "C"
            strike_price = self.short_call_strike

        elif loop_id == 1:
            #on loop one we get the call with strike price k2
            curr_right = "C"
            strike_price = self.long_call_strike

        elif loop_id == 2:
            #on loop one we get the PUT call with strike price k1
            curr_right = "P"
            strike_price = self.short_put_strike

        elif loop_id == 3:
            #on loop one we get the PUT call with strike price k1
            curr_right = "P"
            strike_price = self.long_put_strike
        else:
            self.loop_id = None
            self.received_condor_data  = True
            print("Finished getting contract ID's returning")
            return

        contract = Contract()
        contract.symbol = self.ticker
        contract.secType = "OPT"
        contract.exchange = "SMART"
        contract.currency = "USD"
        contract.lastTradeDateOrContractMonth = self.expiry
        contract.strike = strike_price
        contract.right = curr_right
        contract.multiplier = "100"

        
        self.loop_id = loop_id
        self.reqContractDetails(loop_id, contract)

    def contractDetailsEnd(self, reqId: OrderId):
        return super().contractDetailsEnd(reqId)

    #wrapper function - CUSTOMIZED
    def contractDetails(self, reqId, contractDetails):
        if self.loop_id == 0:
            self.short_call_id = contractDetails.contract.conId

        elif self.loop_id == 1:
            self.long_call_id = contractDetails.contract.conId
        
        elif self.loop_id == 2:
            self.short_put_id = contractDetails.contract.conId

        elif self.loop_id == 3:
            self.long_put_id = contractDetails.contract.conId

        #call the get options data for the next option to get
        self.get_condor_options_data(self.loop_id + 1)


    #custom function
    def buy_iron_condor_order(self ,ticker, expiry , short_call_strike, long_call_strike, short_put_strike, long_put_strike, order):
        '''
        Inputs: ticker : string, amount : int
        Orders x irons condors for the given underlying ticker (long only)
        '''
        self.expiry = expiry
        # Outer Options - to cap loss
        self.long_call_strike = long_call_strike
        self.long_put_strike = long_put_strike

        # Inner options to make profit
        self.short_call_strike = short_call_strike
        self.short_put_strike = short_put_strike


        self.order = order
        self.ticker = ticker

        # Get the contract details for the options
        # TODO REWORK THIS INTO A LOOP, and USE CONTRACT DETAILS END ON THE SLEEP

        # Long CALL  - Outter 1
        contract = Contract()
        contract.symbol = self.ticker
        contract.secType = "OPT"
        contract.exchange = "SMART"
        contract.currency = "USD"
        contract.lastTradeDateOrContractMonth = self.expiry
        contract.strike = long_call_strike
        contract.right = "C"
        contract.multiplier = "100"

        self.reqContractDetails()

        # Long PUT  - Outter 2
        contract = Contract()
        contract.symbol = self.ticker
        contract.secType = "OPT"
        contract.exchange = "SMART"
        contract.currency = "USD"
        contract.lastTradeDateOrContractMonth = self.expiry
        contract.strike = long_put_strike
        contract.right = "P"
        contract.multiplier = "100"



        

        
        self.loop_id = loop_id
        self.reqContractDetails(loop_id, contract)

        self.get_condor_options_data(0)

        while 

        # Define the combo contract
        contract = Contract()
        contract.symbol = ticker
        contract.secType = "BAG"
        contract.currency = "USD"
        contract.exchange = "SMART"

        #We want to wait for options data before continue
        while(self.received_condor_data is False):
            time.sleep(1)
        
        #reset the wait flag
        self.received_condor_data = False

        # Short Option 1
        shortcall = ComboLeg()
        shortcall.conId = self.short_call_id
        shortcall.ratio = 1
        shortcall.action = "SELL"

        # Short Option 2
        shortPut = ComboLeg()
        shortPut.conId = self.short_put_id
        shortPut.ratio = 1
        shortPut.action = "SELL"

        #Long Option 1
        longPut = ComboLeg()
        longPut.conId = self.long_put_id
        longPut.ratio = 1
        longPut.action = "BUY"

        #Long Option 2
        longCall = ComboLeg()
        longCall.conId = self.long_call_id
        longCall.ratio = 1
        longCall.action = "BUY"


        # Add the legs to the combo
        contract.comboLegs = [shortPut, shortcall, longCall, longPut]
        self.contract = contract

        self.reqIds(1) #places the order

    #CALL BACK
    def nextValidId(self, orderId):
        ''' Provides the next order ID '''
       
        #place the order
        if(self.contract is not None and self.order is not None):
            self.placeOrder(orderId, self.contract, self.order)
        else:
            print("Please set an order with the provided functions")



    def openOrder(self, orderId: int, contract: Contract, order: Order, orderState: OrderState):
        print('Order status: '.format(orderState.status))
        print('Commission charged: '.format(orderState.commission))

        #check the position status after the order has been submitted
        self.reqPositions()
    

    def orderStatus(self,order_id, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice):
        ''' Check the status of the submitted order '''
        print('Number of filled positions: {}'.format(filled))
        print('Average fill price: {}'.format(avgFillPrice))
    
    
    def position(self, account, contract, pos, avgCost):
        ''' Read information about open positions '''
        print('Position in {}: {}'.format(contract.symbol, pos))
    

def main():
    # Create the client and connect to TWS
    client = IronCondor('127.0.0.1', 7497, 0)
    # Define the limit order
    order = Order()
    order.action = 'BUY'
    order.totalQuantity = 2
    order.orderType = 'MKT'
    order.transmit = True

    client.buy_iron_condor_order("TSLA" , "20231117", 237.50 , 240.00, 232.50, 230.00, order)

    # # Obtain information about account
    # client.reqAccountSummary(0, 'All','AccountType,AvailableFunds')
    # time.sleep(2)
    # # Disconnect from TWS
    time.sleep(10)
    client.disconnect()

if __name__ == '__main__':
    main()

