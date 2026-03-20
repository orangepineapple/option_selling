from decimal import Decimal
from threading import Thread
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from time import sleep
from util.discord_notify import send_notif 
from ibapi.order import Order
from typing import Any
from datetime import datetime

import logging

logger = logging.getLogger(__name__)

class BuyRebound(EWrapper, EClient):
    '''
    Used to request any historical stock data from IB
    '''
    def __init__(self, addr, port, client_id):
        EWrapper.__init__(self)
        EClient. __init__(self, self)
        # Connect to TWS
        self.connect(addr, port, client_id)

        self.watchlist = []

        # Utility + Connectivity
        self.failed_to_connect = False
        self.ticker_unavailable = set()

        # Order Placement
        self.order_ids = []
        self.order_information = {}
        self.filled_order_count = 0
        self.failed_orders = []

        # Launch the client thread
        thread = Thread(target=self.run)
        thread.start()
        sleep(0.5) # give this thread some time to start    

    def send_market_orders(self, tickers, quantity) -> list[dict[str, Any]]:
        '''
        Sends market orders for each ticker inputted into the list
        '''
        # Request all the order ids
        for i in range(len(tickers)):
            self.reqIds(1)

        # wait for order ids
        while len(self.order_ids) < len(tickers):
            sleep(0.5)

        for i , ticker in enumerate(tickers):
            contract = Contract()
            contract.symbol = ticker
            contract.secType = "STK"
            contract.currency = "USD"
            contract.exchange = "SMART"

            order = Order()
            order.action = 'BUY'
            order.totalQuantity = quantity[i]
            order.orderType = 'MKT'
            order.transmit = True
            order.orderId = self.order_ids[i]

            self.order_information[order.orderId] = {
                "quantity" : quantity[i],
                "ticker" : ticker,
                "action" : "buy execution",
                "fill_price" : None,
                "datetime" : None,
            }
         
            self.placeOrder(order.orderId, contract, order)
        
        while self.filled_order_count < len(self.order_ids):
            sleep(0.5)
        
        # Reset Counters
        self.order_ids = []
        self.filled_order_count = 0

        return list(self.order_information.values())
    
    ### BUILT IN CALLBACKS
    def nextValidId(self, orderId: int):
        '''
        Gets the orderIds and places them into a list, list is cleared after orders are send
        '''
        self.order_ids.append(orderId)
    
    def orderStatus(self, orderId: int, status: str, filled: Decimal, remaining: Decimal, avgFillPrice: float, permId: int, parentId: int, lastFillPrice: float, clientId: int, whyHeld: str, mktCapPrice: float):
        # when remaining is 0, then avgFillPrice contains the price we actaully finished the order add
        if remaining == 0 and status == "Filled": 
            self.order_information[orderId]["fill_price"] = avgFillPrice
            self.order_information[orderId]["datetime"] = datetime.now()
            self.filled_order_count += 1

    def error(self, reqId: int, errorCode: int, errorString: str, advancedOrderRejectJson=""):
        # client not connected error
        if errorCode == 504: 
            if not self.failed_to_connect:
                send_notif("@everyone gateway is not connected, please restart manually")    
            self.failed_to_connect = True
        # contract not found
        elif errorCode == 200: 
            print("contract not found")
            self.failed_orders.append(reqId)
        else:
            print(errorCode, errorString)