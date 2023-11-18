from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
from threading import Thread
import time
import datetime
import sys
import numpy as np
import logging
from threading import Event

logging.basicConfig(level=logging.INFO)


class TestApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.daily_roll = None
        self.next_order_id = 0
        self.vix_spot_price = None
        self.vix_future_price = None
        self.e_mini_price = None
        self.num_business_days = 9
        self.hedge_ratio = 2.61
        self.position = 0
        self.VIX_FUTURE_REQ_ID = 1001
        self.VIX_INDEX_REQ_ID = 1002
        self.EMINI_REQ_ID = 1003
        self.vix_future_contract = None
        self.e_mini_contract = None
        self.position_days = 0
        self.b_t = None
        self.vx_outstanding = 0
        self.emini_outstanding = 0

        self.symbolmap = {
            1001: 'VX',
            1002: 'VIX',
            1003: 'EMINI'
        }

        self.data_received_events = {
            self.VIX_FUTURE_REQ_ID: Event(),
            self.VIX_INDEX_REQ_ID: Event(),
            self.EMINI_REQ_ID: Event()
        }
        self.data_flag = 0

    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        self.next_order_id = orderId
        self.start()

    def create_vix_future_contract(self):
        contract = Contract()
        contract.localSymbol = "VXF4"
        contract.tradingClass = 'VX'
        contract.secType = "FUT"
        contract.exchange = "CFE"
        contract.currency = "USD"
        contract.lastTradeDateOrContractMonth = "202401"
        return contract

    def create_vix_spot_contract(self):
        contract = Contract()
        contract.symbol = "VIX"
        contract.secType = "IND"
        contract.exchange = "CBOE"
        contract.currency = "USD"
        contract.multiplier = "100"
        return contract

    def create_emini_contract(self):
        contract = Contract()
        contract.localSymbol = 'ESZ3'
        contract.secType = 'FUT'
        contract.exchange = "CME"
        contract.currency = "USD"
        contract.multiplier = "50"
        contract.tradingClass = 'ES'
        return contract

    def start(self):

        if not self.check_if_market_is_open():
            logging.info('WARNING: MARKET IS CLOSED!')
            self.disconnect()
        self.reqMarketDataType(3)
        self.vix_future_contract = self.create_vix_future_contract()
        self.e_mini_contract = self.create_emini_contract()
        self.vix_spot_contract = self.create_vix_spot_contract()

        self.get_todays_open_price(self.vix_spot_contract, self.VIX_INDEX_REQ_ID)
        self.get_todays_open_price(self.vix_future_contract, self.VIX_FUTURE_REQ_ID)
        self.get_todays_open_price(self.e_mini_contract, self.EMINI_REQ_ID)

        all_data_received = all(self.data_received_events.values())

        while not all_data_received:
            time.sleep(0.01)

    def create_contract(self, symbol, sec_type, exchange, currency):
        contract = Contract()
        contract.symbol = symbol
        contract.secType = sec_type
        contract.exchange = exchange
        contract.currency = currency
        return contract

    def get_todays_open_price(self, contract, reqId):

        queryTime = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime("%Y%m%d %H:%M:%S")
        self.reqHistoricalData(reqId=reqId,
                               contract=contract,
                               endDateTime=queryTime,
                               durationStr="1 D",
                               barSizeSetting="1 day",
                               whatToShow="TRADES",
                               useRTH=1,
                               formatDate=1,
                               keepUpToDate=False,
                               chartOptions=[])

    def get_market_hours(self):
        # hardcoded to be replaced
        return {
            'open': datetime.time(hour=9, minute=30),
            'close': datetime.time(hour=16, minute=0)
        }

    def check_if_market_is_open(self, ):
        market_hours = self.get_market_hours()
        current_time = datetime.datetime.now().time()
        return True
        # if market_hours['open'] <= current_time <= market_hours['close']:
        #     return True
        # else:
        #     return False

    def historicalData(self, reqId, bar):
        logging.info(f"Received historical data for reqId {reqId} ({self.symbolmap[reqId]}): Open price {bar.open}")
        print(f"Open price for reqId {reqId} {self.symbolmap[reqId]}: {bar.open}")

        if reqId == self.VIX_FUTURE_REQ_ID:
            self.vix_future_price = bar.open
            print('vix fut:', bar.open)

        elif reqId == self.VIX_INDEX_REQ_ID:
            self.vix_spot_price = bar.open
            print('vix_spot', bar.open)

        elif reqId == self.EMINI_REQ_ID:
            self.e_mini_price = bar.open
            print('emini', bar.open)

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        logging.info(f"Historical data download completed for reqId {reqId}")
        self.data_received_events[reqId].set()
        time.sleep(1)

        if all([event_flag.is_set() for event_flag in self.data_received_events.values()]):
            time.sleep(1)
            self.run_strategy()
        else:
            logging.info('PRICES NOT RECEIVED')

    def calculate_basis_and_roll(self):
        self.b_t = self.vix_future_price / self.vix_spot_price - 1
        self.daily_roll = (self.vix_future_price - self.vix_spot_price) / self.num_business_days

    def run_strategy(self):

        if self.vix_future_price is not None and self.vix_spot_price is not None:
            self.calculate_basis_and_roll()

            if self.b_t > 0 and self.daily_roll > 0.10:
                self.short_vix_futures()
                self.next_order_id += 1

            elif self.b_t < 0 and self.daily_roll < -0.10:
                self.long_vix_futures()
                self.next_order_id += 1

        else:
            logging.info('PRICES NOT RECEIVED- CHECK FAILED')

    def short_vix_futures(self, size=25):
        if self.position != -1:
            self.exit_position()
            self.place_order(self.vix_future_contract, -size)
            self.place_order(self.e_mini_contract, self.calculate_hedge_quantity(-size))
            self.emini_outstanding += self.calculate_hedge_quantity(size)
            self.vx_outstanding -= size
            self.position = -1

    def long_vix_futures(self, size=25):
        if self.position != 1:
            self.exit_position()
            self.place_order(self.vix_future_contract, size)
            self.place_order(self.e_mini_contract, self.calculate_hedge_quantity(size))
            self.emini_outstanding -= self.calculate_hedge_quantity(size)
            self.vx_outstanding += size
            self.position = 1

    def calculate_hedge_quantity(self, vx_quantity):
        vx_value = vx_quantity * self.vix_future_price
        emini_quantity = -(np.floor(vx_value / self.e_mini_price * self.hedge_ratio))
        return emini_quantity

    def exit_position(self):
        if self.position == -1 and self.daily_roll < 0.05:
            self.place_order(self.vix_future_contract, 1)
            self.place_order(self.e_mini_contract, -self.hedge_ratio * self.position)
            self.position = 0
        elif self.position == 1 and self.daily_roll > -0.05:
            self.place_order(self.vix_future_contract, -1)
            self.place_order(self.e_mini_contract, -self.hedge_ratio * self.position)
            self.position = 0
        elif self.position != 0 and self.position_days >= self.num_business_days:
            self.place_order(self.vix_future_contract, -self.position)
            self.place_order(self.e_mini_contract, self.hedge_ratio * self.position)
            self.position = 0

    def place_order(self, contract, quantity):
        order = Order()
        order.action = "BUY" if quantity > 0 else "SELL"
        order.orderType = "MKT"
        order.eTradeOnly = False
        order.firmQuoteOnly = False
        order.totalQuantity = abs(quantity)
        self.placeOrder(self.next_order_id, contract, order)
        self.next_order_id += 1

    def error(self, reqId, errorCode, errorString):
        if reqId == -1:
            entity = 'GENERAL LOG'
        else:
            entity = self.symbolmap.get(reqId)
        print("Error: ", reqId, " ", errorCode, " ", errorString, 'for', entity)

    def contractDetails(self, reqId, contractDetails):
        print("contractDetails: ", reqId, " ", contractDetails)


def main():
    app = TestApp()
    app.connect("127.0.0.1", 7497, clientId=0)
    api_thread = Thread(target=app.run)
    api_thread.start()
    time.sleep(1)


if __name__ == "__main__":
    logging.info("Starting the IBKR Trading Application")
    main()
