from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
from threading import Thread
import time
import datetime
import sys
import logging
logging.basicConfig(level=logging.INFO)


class TestApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
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
        self.symbolmap = {
            1001: 'VX',
            1002: 'VIX',
            1003: 'EMINI'
        }
        self.data_received = False

    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        self.next_order_id = orderId
        self.start()

    def create_vix_future_contract(self):
        contract = Contract()
        contract.localSymbol = "VXZ3"
        contract.tradingClass = 'VX'
        contract.secType = "FUT"
        contract.exchange = "CFE"
        contract.currency = "USD"
        contract.lastTradeDateOrContractMonth = "202312"
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
        self.reqMarketDataType(3)
        self.vix_future_contract = self.create_vix_future_contract()
        self.e_mini_contract = self.create_emini_contract()
        self.vix_spot_contract = self.create_vix_spot_contract()

        self.get_todays_open_price(self.vix_spot_contract, self.VIX_INDEX_REQ_ID)
        self.get_todays_open_price(self.vix_future_contract, self.VIX_FUTURE_REQ_ID)
        self.get_todays_open_price(self.e_mini_contract, self.EMINI_REQ_ID)

        timeout = 10  # seconds
        start_time = time.time()

        if all([self.vix_future_price, self.vix_spot_price, self.e_mini_price]):
            self.data_received = True
            logging.info("All necessary historical data received.")

        while not self.data_received and time.time() - start_time < timeout:
            time.sleep(0.001)


        if self.check_if_market_is_open():

            self.run_strategy()

        else:
            print('WARNING: MARKET IS CLOSED! \n' * 10)

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
            print('no prices received')

    def short_vix_futures(self):
        if self.position != -1:
            self.exit_position()
            self.place_order(self.vix_future_contract, -1)
            self.place_order(self.e_mini_contract, self.hedge_ratio)
            self.position = -1

    def long_vix_futures(self):
        if self.position != 1:
            self.exit_position()
            self.place_order(self.vix_future_contract, 1)
            self.place_order(self.e_mini_contract, -self.hedge_ratio)
            self.position = 1

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
