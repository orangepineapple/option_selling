from datetime import datetime, timedelta
from threading import Thread
from ibapi.client import EClient
from ibapi.common import TickAttrib, TickerId
from ibapi.ticktype import TickType
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract, ContractDetails
import time


class Option():
    '''
    Represents a single options contract.

    Populated in two stages:
      1. get_options_chain()     — sets ticker, expiry, strike, right
      2. get_prices_and_greeks() — fills in bid, ask, mid and all greeks
    '''
    def __init__(self, ticker, expiry, strike, right):
        # --- identity ---
        self.ticker = ticker
        self.expiry = expiry    # datetime
        self.strike = strike    # float
        self.right  = right     # 'P' or 'C'

        # --- populated by get_prices_and_greeks() ---
        self.bid   = None
        self.ask   = None
        self.mid   = None
        self.iv    = None
        self.delta = None
        self.gamma = None
        self.vega  = None
        self.theta = None

    def is_priced(self):
        ''' Returns True if price data has been populated '''
        return self.mid is not None

    def __str__(self):
        return (
            f"{self.ticker} {self.right} "
            f"exp={self.expiry.strftime('%Y%m%d')} "
            f"strike={self.strike} "
            f"mid={self.mid} delta={self.delta}"
        )

    def __repr__(self):
        return self.__str__()


class OptionsChainPrices(EWrapper, EClient):
    '''
    IBKR API client for fetching options chains, prices, and greeks.

    Designed for sequential multi-security use — call get_options_chain()
    then get_prices_and_greeks() for each ticker in turn. State is reset
    between tickers automatically.

    req_id allocation:
      - _req_id_counter is a global monotonically increasing counter that
        is NEVER reset for the lifetime of the connection.
      - Every request (underlying price, chain details, option prices, IV)
        gets its own unique ID from next_req_id().
      - Callbacks identify which request they belong to by looking up the
        req_id in registry dicts that map req_id -> Option object directly.
      - All lookups in callbacks are O(1) dict gets — no linear search.
    '''

    def __init__(self, addr, port, client_id):
        EWrapper.__init__(self)
        EClient.__init__(self, self)

        self.connect(addr, port, client_id)

        # Global req_id counter — never reset between tickers
        self._req_id_counter = 0

        # Only bid (1) and ask (2) — last price is stale for options
        self.TRACKED_PRICE_FIELDS = {1, 2}

        self._reset_per_ticker_state()

        thread = Thread(target=self.run)
        thread.start()
        time.sleep(0.5)

    # ------------------------------------------------------------------
    # req_id allocation
    # ------------------------------------------------------------------

    def next_req_id(self):
        '''
        Allocate and return the next unique request ID.
        The only place req_ids are ever created — never hardcode one.
        '''
        self._req_id_counter += 1
        return self._req_id_counter

    # ------------------------------------------------------------------
    # Per-ticker state
    # ------------------------------------------------------------------

    def _reset_per_ticker_state(self):
        '''
        Clear all state that belongs to a single ticker.
        Called at the start of get_options_chain() for each new security.
        _req_id_counter is intentionally excluded — it never resets.
        '''
        self.ticker        = None
        self.target_expiry = datetime.today() + timedelta(days=45) #Default

        # Underlying price for the current ticker
        self.underlying_price = None

        # Set to True when contractDetailsEnd fires
        self._chain_complete = None

        # The options chain as a flat list of Option objects
        self.chain: list[Option] = []

        # Closest expiry dates bracketing target_expiry
        self.closest_low_date  = datetime.today() - timedelta(days=365)
        self.closest_high_date = datetime.today() + timedelta(days=365)

        # req_id for the underlying mkt data request
        self._underlying_req_id = None

        # req_id for the contract details request
        self._chain_req_id = None

        # Maps price req_id -> Option object
        # This is the core of the callback routing — when IBKR sends back
        # a tick for req_id 42, we look up which Option object that belongs
        # to in O(1) and write directly onto it. No hashes, no parsing.
        self._req_id_to_option: dict[int, Option] = {}

        # Maps IV req_id -> Option object (separate namespace from price req_ids)
        self._iv_req_id_to_option: dict[int, Option] = {}

        # Tracks how many price fields are still expected per req_id.
        # Initialised to 2 (bid + ask) per option, deleted when it hits 0
        # so stale/duplicate ticks can't re-decrement the counter.
        self._price_track: dict[int, int] = {}

        # Shared countdown polled by the while loops in public methods
        self._remaining = None

        self.high_date_option = None
        self.low_date_option = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_contract(self, option: Option) -> Contract:
        ''' Build an IBKR Contract object from an Option '''
        contract = Contract()
        contract.symbol     = option.ticker
        contract.secType    = 'OPT'
        contract.exchange   = 'CBOE'
        contract.currency   = 'USD'
        contract.lastTradeDateOrContractMonth = option.expiry.strftime('%Y%m%d')
        contract.strike     = option.strike
        contract.right      = option.right
        contract.multiplier = '100'
        return contract

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_options_chain(self, ticker: str, target_expiry_date: datetime):
        '''
        Fetch the full options chain for a single ticker.
        Safe to call multiple times sequentially for different tickers.

        Returns:
            chain            : List[Option]  — all contracts, unpriced
            closest_high_date: datetime      — nearest expiry >= target
            closest_low_date : datetime      — nearest expiry <  target
        '''
        self._reset_per_ticker_state()

        self.ticker        = ticker
        self.target_expiry = target_expiry_date
        print(f"[{ticker}] requesting chain, target expiry: {target_expiry_date.date()}")

        # --- Underlying price ---
        self._underlying_req_id = self.next_req_id()
        underlying          = Contract()
        underlying.symbol   = ticker
        underlying.secType  = 'STK'
        underlying.exchange = 'SMART'
        underlying.currency = 'USD'
        
        # Get the price, average IV , and 30-day Historical IV
        self.reqMktData(self._underlying_req_id, underlying, '106,104', True, False, [])

        # --- Full options chain ---
        self._chain_req_id = self.next_req_id()
        opt_contract          = Contract()
        opt_contract.symbol   = ticker
        opt_contract.secType  = 'OPT'
        opt_contract.currency = 'USD'
        opt_contract.exchange = 'CBOE'
        self.reqContractDetails(self._chain_req_id, opt_contract)

        while self.underlying_price is None:
            time.sleep(1)
        print(f"[{ticker}] underlying price: {self.underlying_price}")

        while self._chain_complete is None:
            time.sleep(1)
        print(f"[{ticker}] chain complete — {len(self.chain)} contracts")

        return self.chain, self.high_date_option, self.low_date_option

    # def get_prices_and_greeks(self, options: list[Option]):
        
    #     # TODO FIX BUGS LEFT IN BY AI

    #     if self.underlying_price is None:
    #         raise RuntimeError(
    #             "underlying_price is not set. Call get_options_chain() first "
    #             "or set app.underlying_price manually before calling this."
    #         )

    #     self._remaining = len(options)

    #     # --- First pass: request bid/ask for each option ---
    #     for option in options:
    #         rid = self.next_req_id()
    #         self._req_id_to_option[rid] = option   # map req_id -> Option object
    #         self._price_track[rid]      = 2         # expecting bid + ask
    #         self.reqMktData(rid, self._build_contract(option), '', True, False, [])

    #     while self._remaining > 0:
    #         print(f"[{self.ticker}] price requests remaining: {self._remaining}")
    #         time.sleep(1)
    #     print(f"[{self.ticker}] all prices received — requesting greeks")

    #     # --- Second pass: IV / greeks for options with a valid mid ---
    #     priced  = [o for o in options if o.mid is not None and o.mid > 0]
    #     skipped = len(options) - len(priced)
    #     if skipped:
    #         print(f"[{self.ticker}] skipping greeks for {skipped} options with no valid mid")

    #     self._remaining = len(priced)

    #     for option in priced:
    #         iv_rid = self.next_req_id()
    #         self._iv_req_id_to_option[iv_rid] = option  # map IV req_id -> Option object
    #         self.calculateImpliedVolatility(
    #             iv_rid,
    #             self._build_contract(option),
    #             option.mid,
    #             self.underlying_price,
    #             []
    #         )

    #     while self._remaining > 0:
    #         print(f"[{self.ticker}] greek requests remaining: {self._remaining}")
    #         time.sleep(1)
    #     print(f"[{self.ticker}] done — all greeks received")

    #     return options

    # ------------------------------------------------------------------
    # IBKR Callbacks
    # ------------------------------------------------------------------

    def contractDetails(self, reqId: TickerId, contractDetails: ContractDetails):
        '''
        Called once per contract when reqContractDetails returns.
        Creates an Option object for each contract and appends to self.chain.
        '''
        if reqId != self._chain_req_id:
            print("Request doesnt line up with stored variable")
            return

        expiry_date = datetime.strptime(
            contractDetails.contract.lastTradeDateOrContractMonth, '%Y%m%d'
        )
        curr_opt = Option(
            ticker = self.ticker,
            expiry = expiry_date,
            strike = contractDetails.contract.strike,
            right  = contractDetails.contract.right
        )
        self.chain.append(curr_opt)

        # Track the two expiry dates bracketing the target
        if expiry_date >= self.target_expiry:
            if expiry_date < self.closest_high_date:
                self.closest_high_date = expiry_date
                self.high_date_option = curr_opt
        else:
            if expiry_date > self.closest_low_date:
                self.closest_low_date = expiry_date
                self.low_date_option = curr_opt

    def contractDetailsEnd(self, reqId):
        if reqId == self._chain_req_id:
            self._chain_complete = True

    def tickPrice(self, reqId: int, tickType: int, price: float, attrib: TickAttrib):
    
        '''
        Receives price ticks for the underlying and options.

        For options: looks up the Option object directly from _req_id_to_option
        in O(1), writes bid/ask onto it, and calculates mid once both arrive.
        '''
        # Underlying tick
        if reqId == self._underlying_req_id:
            if tickType == 4:  # LAST
                self.underlying_price = price
            return

        if tickType not in self.TRACKED_PRICE_FIELDS:
            return

        # Guard against stale/duplicate ticks after we've finished processing
        # this req_id (it gets deleted from _price_track when count hits 0)
        if reqId not in self._price_track:
            return

        # O(1) lookup — this is why we map req_id -> Option directly
        option = self._req_id_to_option.get(reqId)
        if option is None:
            return

        if tickType == 1:
            option.bid = price
        elif tickType == 2:
            option.ask = price

        self._price_track[reqId] -= 1

        if self._price_track[reqId] == 0:
            del self._price_track[reqId]

            # IBKR sends -1 to mean "no market" — treat as invalid
            if option.bid is not None and option.ask is not None \
                    and option.bid >= 0 and option.ask >= 0:
                option.mid = round((option.bid + option.ask) / 2, 4)
            else:
                option.mid = None
                print(f"no valid market for {option} — mid set to None")

            self._remaining -= 1

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

    def tickOptionComputation(self, reqId, tickType, tickAttrib,
                              impliedVol, delta, optPrice, pvDividend,
                              gamma, vega, theta, undPrice):
        '''

        '''
        if tickType != 13:
            return

        # Check price req_id first, then IV req_id — both are O(1)
        option = self._req_id_to_option.get(reqId) \
              or self._iv_req_id_to_option.get(reqId)

        if option is None:
            return


        self._remaining -= 1


    def error(self, reqId: TickerId, errorTime: TickerId, errorCode: TickerId, errorString: str, advancedOrderRejectJson=""):

        print(errorCode, errorString)