import collections
import concurrent.futures
import itertools
from pathlib import Path

from kucoin.exceptions import *

from transaction_helper import TransactionHelper
from utility import *
from constants import *

log = logging.getLogger(__name__)


class Helper(object):
    TRADE_PRECISION = 'tradePrecision'
    TRADE_THRESHOLD = 0.008

    def __init__(self, client, market_1, market_2):
        self.client = client
        self.market_1 = market_1
        self.market_2 = market_2
        self.base_symbol = "%s-%s" % (market_2, market_1)
        self.coins_info = self.load_coins_info()
        self.market_1_price = None

        self.balance_cache = self.get_all_balances()
        self.profit = 0.0
        self.transaction_helper = TransactionHelper(client)
        self.min_amount = self.get_min_order_amount()
        self.trade_ratio = 0.49
        self.trade_static = {'success': {}, 'failure': {}}

    def load_coins_info(self):
        try:
            return dict((coin['coin'], coin) for coin in self.client.get_coin_list())
        except (KucoinAPIException, KucoinRequestException) as e:
            log.error('Error while loading coin information')
            log.error(e, exc_info=True)

    def get_trading_ticker(self, market, coin):
        symbol = '%s-%s' % (coin, market)
        try:
            return self.client.get_trading_ticker(market, symbol)
        except (KucoinAPIException, KucoinRequestException) as e:
            log.error('Error while loading trading ticker for %s' % symbol.split('-')[0])
            log.error(e, exc_info=True)

    def get_order_book(self, symbol, group=None, limit=None):
        response = call_api_with_retry(self.client.get_order_book, symbol, group, limit)
        return {'symbol': symbol, 'BUY': response['BUY'][0][:2], 'SELL': response['SELL'][0][:2]}

    def get_symbol_dealt_orders(self, symbol, order_type, limit):
        try:
            return self.client.get_symbol_dealt_orders(symbol, order_type=order_type, limit=limit)['datas']
        except (KucoinAPIException, KucoinRequestException) as e:
            log.error('Error while getting symbol dealt orders')
            log.error(e)

    def get_dealt_orders(self, symbols_types, limit=3):
        dealt_orders = []
        for s, t in symbols_types:
            response = self.get_symbol_dealt_orders(symbol=s, order_type=t, limit=limit)
            for order in response:
                dealt_orders.append(order['orderOid'])
        return dealt_orders

    def get_order_details(self, symbol, order_type, order_id):
        try:
            response = self.client.get_order_details(symbol, order_type, order_id=order_id)
            if response:
                return response
            else:
                raise KucoinRequestException("Response is empty. Order id: %s" % order_id)
        except (KucoinAPIException, KucoinRequestException) as e:
            log.error('Error while getting order detail.')
            log.error(e, exc_info=True)

    def get_coin_balance(self, coin):
        response = call_api_with_retry(self.client.get_coin_balance, coin)
        if response["coinType"] == coin:
            return response["balance"]
        else:
            raise ValueError("Invalid Response %s" % response)

    def get_all_balances(self):
        """
        Get all balances
        :return: A dict contains balances for all coins
        """
        balances_cache = {}
        response = call_api_with_retry(self.client.get_all_balances)
        for coin in response:
            balances_cache[coin['coinType']] = coin['balance']
        log_json_utils(log.info, message="Finished refreshing balance cache", data=balances_cache)
        return balances_cache

    def get_feed_order(self, symbol, order_type, trade_precision):
        price, amount, volume = self.client.get_order_book(symbol)[order_type][0]

        if order_type == "BUY":
            price_d = round(price - 1 / (10 ** trade_precision), trade_precision)
        elif order_type == "SELL":
            price_d = round(price + 1 / (10 ** trade_precision), trade_precision)
        else:
            raise ValueError("Invalid order_type=%order_type, must be either buy or sell." % order_type)
        amount_d = round(amount, trade_precision)
        volume_d = round(price * amount, trade_precision)

        return price_d, amount_d, volume_d

    def generate_order(self, ticker, order_type, trade_precision, floating_price=False, floating_rate=10):
        price, amount = ticker[order_type][0], ticker[order_type][1]
        if not floating_price:
            return price, amount
        if not (0 <= floating_rate <= 100):
            raise ValueError("Invalid floating rate. Must be between 0 and 100.")
        if order_type == 'BUY':
            new_price = round(price * (1.0 - 0.00001 * floating_rate), trade_precision)
        elif order_type == 'SELL':
            new_price = round(price * (1.0 + 0.00001 * floating_rate), trade_precision)
        else:
            raise ValueError("Invalid order_type=%s, must be either buy or sell." % order_type)
        return new_price, amount

    def get_trade_amount(self, order_amount, coin, coin_price, market_2_price, trade_precision):
        market_1_bal, market_2_bal = self.balance_cache[self.market_1], self.balance_cache[self.market_2]
        coin_bal = self.balance_cache[coin]

        # Calculate balances based on market_1
        coin_worth = coin_bal * coin_price
        market_2_worth = market_2_bal * market_2_price
        min_bal = min([coin_worth, market_2_worth, market_1_bal]) * self.trade_ratio
        trade_amount = min(order_amount, min_bal / coin_price)
        log_json_utils(log.info, message="Getting trade amount",
                       balances={self.market_1: market_1_bal, self.market_2: market_2_worth, coin: coin_worth}, trade_amount=trade_amount)
        log.info("Minimum balance is %.8f, coin price is %.8f, trade amount is %.8f" % (min_bal, coin_price, trade_amount))
        return round(trade_amount, trade_precision)

    def get_min_trade_amount(self, coin_trade_precision):
        tickSize = 10. ** (-coin_trade_precision)
        return round(tickSize * 30, coin_trade_precision)

    def track_3_combo_orders(self, orderOid_dict):
        recent_dealt_orders = set(self.get_dealt_orders([(v['symbol'], v['type']) for k, v in orderOid_dict.items()]))
        target_orders = set(orderOid_dict.keys())
        print(target_orders - recent_dealt_orders)
        return target_orders - recent_dealt_orders

    def cancel_active_orders(self, active_order_set):
        for active_order in active_order_set:
            orderOid = active_order['orderOid']
            symbol = active_order['coinType'] + '-' + active_order['coinTypePair']
            order_type = active_order['type']
            log.info("Cancelling order: %s.", orderOid)
            self.transaction_helper.cancel_active_order(symbol, orderOid, order_type)
            log.info("Successfully cancelled order %s" % orderOid)

    def detect_spread_and_fill(self, coin):
        symbol_1 = '%s-%s' % (coin, self.market_1)
        symbol_2 = '%s-%s' % (coin, self.market_2)
        base_symbol = '%s-%s' % (self.market_2, self.market_1)

        tickers = self.get_orderbook_parallel([symbol_1, symbol_2, base_symbol])
        if not tickers:
            return False
        for ticker in tickers:
            if ticker['symbol'] == symbol_1:
                ticker_1 = ticker
            elif ticker['symbol'] == symbol_2:
                ticker_2 = ticker
            elif ticker['symbol'] == base_symbol:
                base_ticker = ticker

        spread_result = self.get_spread(ticker_1, ticker_2, base_ticker, coin)
        make_spread = False

        if not spread_result:
            return False
            # spread_result = self.make_spread(ticker_1, ticker_2, base_ticker, coin)
            # if not spread_result:
            #     return False
            # make_spread = True

        spread, direction = spread_result[0], spread_result[1]
        # Trade precision is the minimum order quantity increment for this currency.
        # For market currencies, it is the minimum order price increment for all tokens trading on it.
        trade_precision_1 = self.coins_info[self.market_1][self.TRADE_PRECISION]
        trade_precision_2 = self.coins_info[self.market_2][self.TRADE_PRECISION]
        coin_trade_precision = self.coins_info[coin][self.TRADE_PRECISION]
        if direction:
            # market_1 -> coin -> market_2 -> market_1

            price_1, amount_1 = self.generate_order(ticker_1, 'SELL', trade_precision_1)
            price_2, amount_2 = self.generate_order(ticker_2, 'BUY', trade_precision_2)
            price_3, amount_3 = self.generate_order(base_ticker, 'BUY', trade_precision_1, floating_price=True,
                                                    floating_rate=100)
            spread_volumes = {symbol_1: amount_1, symbol_2: amount_2, base_symbol: amount_3}
            log_json_utils(log.info, message="Spread volume information", data=spread_volumes)
            amount = min(amount_1, amount_2)
            trade_amount = self.get_trade_amount(amount, coin, price_1, price_3, coin_trade_precision)
            if trade_amount <= max(self.min_amount[symbol_1], self.min_amount[symbol_2]):
                log.info("Failed to create orders for this spread. Order amount too low %f" % trade_amount)
                time.sleep(0.5)
                return False
            order_1_params = {'symbol': symbol_1, 'type': 'BUY', 'amount': trade_amount, 'price': price_1}
            order_2_params = {'symbol': symbol_2, 'type': 'SELL',
                              'amount': round(trade_amount, coin_trade_precision), 'price': price_2}
            order_3_params = {'symbol': base_symbol,
                              'type': 'SELL',
                              'amount': round(price_2 * trade_amount, trade_precision_2),
                              'price': price_3}

            spread = price_2 / price_1 * price_3
            if make_spread:
                return self.create_order_group_helper(spread, order_1_params, order_2_params, order_3_params, direction,
                                                      timeout=210, additional_wait_time=8)
            return self.create_order_group_helper(spread, order_1_params, order_2_params, order_3_params, direction)
        else:
            # currency_2 -> coin -> currency_1 -> currency_2

            price_1, amount_1 = self.generate_order(ticker_2, 'SELL', trade_precision_2)
            price_2, amount_2 = self.generate_order(ticker_1, 'BUY', trade_precision_1)
            price_3, amount_3 = self.generate_order(base_ticker, 'SELL', trade_precision_1, floating_price=True,
                                                    floating_rate=100)
            spread_volumes = {symbol_2: amount_1, symbol_1: amount_2, base_symbol: amount_3}
            log_json_utils(log.info, message="Spread volume information", data=spread_volumes)
            amount = min(amount_1, amount_2)
            trade_amount = self.get_trade_amount(amount, coin, price_2, price_3, coin_trade_precision)
            if trade_amount <= max(self.min_amount[symbol_1], self.min_amount[symbol_2]):
                log.info("Failed to create orders for this spread. Order amount too low %f." % trade_amount)
                time.sleep(0.5)
                return False
            order_1_params = {'symbol': symbol_2, 'type': 'BUY', 'amount': trade_amount, 'price': price_1}
            order_2_params = {'symbol': symbol_1, 'type': 'SELL',
                              'amount': trade_amount, 'price': price_2}
            order_3_params = {'symbol': base_symbol,
                              'type': 'BUY',
                              'amount': round(price_2 * trade_amount / price_3, trade_precision_2),
                              'price': price_3}

            spread = price_2 / price_1 / price_3
            if make_spread:
                return self.create_order_group_helper(spread, order_1_params, order_2_params, order_3_params, direction,
                                                      timeout=210, additional_wait_time=8)

            return self.create_order_group_helper(spread, order_1_params, order_2_params, order_3_params, direction)

    def create_order_group_helper(self, spread, order_1_params, order_2_params, order_3_params, direction, timeout=60, additional_wait_time=0):
        if (spread - 1) > (self.TRADE_THRESHOLD - 0.001):
            # create a new order
            order_params_group = [order_1_params, order_2_params, order_3_params]
            log_json_utils(log.info, message="Creating 3 combo orders", data=order_params_group)
            deal_ratio = self.transaction_helper.deal_parallel_orders(order_params_group, timeout, additional_wait_time)
            if direction:
                profit = order_3_params['amount'] * order_3_params['price'] - order_1_params['amount'] * order_1_params[
                    'price']
            else:
                profit = (order_3_params['amount'] - order_1_params['amount'] * order_1_params['price']) * \
                         order_3_params['price']
            profit = round(profit * deal_ratio, self.coins_info[self.market_1][self.TRADE_PRECISION])
            self.profit += profit

            # Refresh market coin balances after creating orders.
            if self.market_1 != 'USDT':
                if self.market_1_price:
                    log.critical("Current Profit is %.8f USDT" % (self.profit * self.market_1_price))
                else:
                    log.error("Primary market exchange rate is not available")
            else:
                log.critical("Current Profit is %.8f USDT" % self.profit)

            coin = order_1_params['symbol'].split('-')[0]
            if deal_ratio > 0.999:
                log.info("Trade succeeded. Earned %.8f %s" % (profit, self.market_1))
                previous_static = self.trade_static['success'].get(coin, (0, 0))
                self.trade_static['success'][coin] = (previous_static[0] + 1, previous_static[1] + profit)
            else:
                log.error("Trade failed. Earned %.8f %s" % (profit, self.market_1))
                previous_static = self.trade_static['failure'].get(coin, (0, 0))
                self.trade_static['failure'][coin] = (previous_static[0] + 1, previous_static[1] + profit)
                # Refresh balance cache
                if deal_ratio < 0.5:
                    self.balance_cache = self.get_all_balances()
                return False
            return True
        else:
            log.info("Spread lower than minimum requirement." + " {:.2%}".format((spread - 1) / 1.0))
            return False

    def create_order_group_helper_tmp(self, **kwargs):
        ticker_from, ticker_to = kwargs['ticker_from'], kwargs['ticker_to']
        base_ticker = kwargs[BASE_TICKER]
        trade_precision_from, trade_precision_to = kwargs[TRADE_PRECISION_1], kwargs[TRADE_PRECISION_2]
        coin_trade_precision = kwargs['coin_trade_precision']
        trade_direction = kwargs['trade_direction']
        price_1, amount_1 = self.generate_order(ticker_from, 'SELL', trade_precision_from)
        price_2, amount_2 = self.generate_order(ticker_to, 'BUY', trade_precision_to)
        price_3, amount_3 = self.generate_order(base_ticker, 'BUY' if trade_direction else 'SELL', trade_precision_from if trade_direction else trade_precision_to)

        amount = min(amount_1, amount_2)
        trade_balance = self.balance_1 * self.trade_ratio if trade_direction else self.balance_2 * self.trade_ratio
        trade_amount = self.get_trade_amount(trade_balance, amount, price_1, coin_trade_precision)
        if trade_amount <= 1.01:
            log.info("Failed to create orders for this spread. Order amount too low %f." % trade_amount)
            return False
        if trade_amount < self.get_min_trade_amount(coin_trade_precision):
            log.info("No enough balance for %s." % self.market_2)
            return False
        return trade_amount

    def get_spread(self, ticker_1, ticker_2, base_ticker, coin):
        ratio_12 = ticker_2['BUY'][0] / ticker_1['SELL'][0]
        ratio_21 = ticker_1['BUY'][0] / ticker_2['SELL'][0]
        spread_12 = ratio_12 * base_ticker['BUY'][0]
        spread_21 = ratio_21 / base_ticker['SELL'][0]
        if (spread_12 - 1) > self.TRADE_THRESHOLD:
            log.info("%s -> %s -> %s  " % (self.market_1, coin, self.market_2) + "{:.2%} ".format(
                (spread_12 - 1) / 1.0) + "unit: %s" % self.market_1)
            return (spread_12 - 1) / 1.0, True
        if (spread_21 - 1) > self.TRADE_THRESHOLD:
            log.info("%s -> %s -> %s  " % (self.market_2, coin, self.market_1) + "{:.2%} ".format(
                (spread_21 - 1) / 1.0) + "unit: %s" % self.market_1)
            return (spread_21 - 1) / 1.0, False
        return

    def make_spread(self, ticker_1, ticker_2, base_ticker, coin):
        gap_1, gap_2 = ticker_1['BUY'][0] / ticker_1['SELL'][0], ticker_2['BUY'][0] / ticker_2['SELL'][0]
        if gap_1 < gap_2:
            my_price_1 = round((ticker_1['BUY'][0] + ticker_1['SELL'][0]) / 2,
                             self.coins_info[self.market_1][self.TRADE_PRECISION])
            my_price_2 = round((ticker_2['BUY'][0] + ticker_2['SELL'][0]) / 2,
                             self.coins_info[self.market_2][self.TRADE_PRECISION])
            new_spread_12 = my_price_2 / my_price_1 * base_ticker['BUY'][0]
            new_spread_21 = my_price_1 / my_price_2 / base_ticker['SELL'][0]

            if (new_spread_12 - 1) > 0.012:
                spread_message = self.generate_spread_message(self.market_1, self.market_2, coin, new_spread_12, self.market_1)
                log.info(spread_message)
                print(spread_message)
                best_new_spread = new_spread_12
                direction = True
            elif (new_spread_21 - 1) > 0.012:
                spread_message = self.generate_spread_message(self.market_2, self.market_1, coin, new_spread_21, self.market_1)
                log.info(spread_message)
                print(spread_message)
                best_new_spread = new_spread_21
                direction = False
            else:
                return None
            ticker_1['BUY'][0] = ticker_1['SELL'][0] = my_price_1
            ticker_1['BUY'][1] = ticker_1['SELL'][1] = max(ticker_1['BUY'][1], ticker_1['SELL'][1])
            ticker_2['BUY'][0] = ticker_2['SELL'][0] = my_price_2
            ticker_2['BUY'][1] = ticker_2['SELL'][1] = max(ticker_2['BUY'][1], ticker_2['SELL'][1])

            return best_new_spread, direction
        """
        else:
            my_price = round((ticker_2['BUY'][0] + ticker_2['SELL'][0]) / 2,
                             self.coins_info[self.market_2][self.TRADE_PRECISION])
            new_spread_12 = my_price / ticker_1['SELL'][0] * base_ticker['BUY'][0]
            new_spread_21 = ticker_1['BUY'][0] / my_price / base_ticker['SELL'][0]
            ticker_2['BUY'][0] = ticker_2['SELL'][0] = my_price
            ticker_2['BUY'][1] = ticker_2['SELL'][1] = max(ticker_2['BUY'][1], ticker_2['SELL'][1])
            if (new_spread_12 - 1) > 0.01:
                spread_message = self.generate_spread_message(self.market_1, self.market_2, coin, new_spread_12,
                                                              self.market_2)
                log.info(spread_message)
                print(spread_message)
                best_new_spread = new_spread_12
                direction = True
            elif (new_spread_21 - 1) > 0.01:
                spread_message = self.generate_spread_message(self.market_2, self.market_1, coin, new_spread_21,
                                                              self.market_2)
                log.info(spread_message)
                print(spread_message)
                best_new_spread = new_spread_21
                direction = False
            else:
                return None

        ticker_2['BUY'][0] = ticker_2['SELL'][0] = my_price
        ticker_2['BUY'][1] = ticker_2['SELL'][1] = max(ticker_2['BUY'][1], ticker_2['SELL'][1])
        return best_new_spread, direction
        """

    def generate_spread_message(self, source, target, coin, spread, gap_market):
        message = ["Create maker order for %s and %s." % (coin, gap_market), "{0} -> {1} -> {2} ".format(source, coin, target),
                   "spread {:.2%}  ".format((spread - 1) / 1.0)]
        return ' '.join(message)

    def calculate_spread_with_best_tick(self, ticker_1, ticker_2, base_ticker, coin):
        ratio_12 = ticker_2['BUY'][0] / ticker_1['SELL'][0]
        ratio_21 = ticker_1['BUY'][0] / ticker_2['SELL'][0]
        spread_12 = ratio_12 * base_ticker['BUY'][0]
        spread_21 = ratio_21 / base_ticker['SELL'][0]
        best_indices_1, best_indices_2 = (0, 0), (0, 0) # (BUY, SELL)
        while (spread_12 - 1) > 0.008:
            ratio_12 = ticker_2['BUY'][best_indices_2[0]] / ticker_1['SELL'][best_indices_1[1]]
            ratio_21 = ticker_1['BUY'][best_indices_1[0]] / ticker_2['SELL'][best_indices_2[0]]
            spread_12 = ratio_12 * base_ticker['BUY'][0]
            spread_21 = ratio_21 / base_ticker['SELL'][0]

    def get_currencies(self, coin=None):
        return call_api_with_retry(self.client.get_currencies, coin)['rates']

    def idle_executor(self):
        log.info("Idle executors in order to keep connections alive.")
        self.transaction_helper.idle_all_executors()

    def fill_balances_gap(self, scan_coins):
        """
        Check market coin balances and target coins, if there is a hugh amount difference, try fix it.

        :return:
        """
        log.info("Balancing coin balances.")
        balance = {}
        usd_balance = {}
        full_coin_list = [self.market_1, self.market_2] + scan_coins
        currency_count = len(full_coin_list)
        exchange_rate = call_api_with_retry(self.client.get_currencies, list(full_coin_list))
        exchange_rate = {k: float(v['USD']) for k, v in exchange_rate['rates'].items()}
        for coin in full_coin_list:
            response = call_api_with_retry(self.client.get_coin_balance, coin)
            balance[coin] = float(response['balance'])
            usd_value = exchange_rate[coin] * balance[coin]
            usd_balance[coin] = usd_value

        self.market_1_price = exchange_rate[self.market_1]

        avg_usd_balance = sum(usd_balance.values()) / currency_count
        log.info("Listing USD valued balances for all trading coins.")
        log_json_utils(log.info, **usd_balance)

        if not self.coins_info:
            log.info("Loading coins information")
            self.coins_info = call_api_with_retry(self.load_coins_info)

        low_market, high_market = self.compare_market_balances(usd_balance)
        # Sell over-valued scan coins
        for coin in scan_coins:
            if usd_balance[coin] / avg_usd_balance > 1.25:
                diff = usd_balance[coin] - avg_usd_balance
                amount = round(diff / exchange_rate[coin], self.coins_info[coin]['tradePrecision'])
                tmp_symbol = '%s-%s' % (coin, low_market)
                ticker = call_api_with_retry(self.client.get_order_book, tmp_symbol, limit=1)
                order_params = {'symbol': tmp_symbol, 'type': 'SELL', 'amount': amount, 'price': ticker['BUY'][0][0]}
                self.transaction_helper.create_and_track_single_order(order_params)
                usd_balance[coin], usd_balance[low_market] = usd_balance[coin] - diff, usd_balance[low_market] + diff

        low_market, high_market = self.compare_market_balances(usd_balance)
        # Buy under-valued scan coins
        for coin in scan_coins:
            if usd_balance[coin] / avg_usd_balance < 0.60:
                diff = avg_usd_balance - usd_balance[coin]
                amount = round(diff / exchange_rate[coin], self.coins_info[coin]['tradePrecision'])
                tmp_symbol = '%s-%s' % (coin, high_market)
                ticker = call_api_with_retry(self.client.get_order_book, tmp_symbol, limit=1)
                order_params = {'symbol': tmp_symbol, 'type': 'BUY', 'amount': amount, 'price': ticker['SELL'][0][0]}
                self.transaction_helper.create_and_track_single_order(order_params)
                usd_balance[coin], usd_balance[high_market] = usd_balance[coin] + diff, usd_balance[high_market] - diff

        # Balance two market coins
        trade_precision = self.coins_info[self.market_2]['tradePrecision']
        if usd_balance[self.market_1] / usd_balance[self.market_2] > 1.25:
            diff = (usd_balance[self.market_1] - usd_balance[self.market_2]) / 2.0
            amount = round(diff / exchange_rate[self.market_2], trade_precision)
            tmp_symbol = '%s-%s' % (self.market_2, self.market_1)
            ticker = call_api_with_retry(self.client.get_order_book, tmp_symbol, limit=1)
            new_price = round(ticker['BUY'][0][0] * 0.9995, trade_precision)
            order_params = {'symbol': tmp_symbol, 'type': 'BUY', 'amount': amount, 'price': new_price}
            self.transaction_helper.create_and_track_single_order(order_params)
            usd_balance[self.market_1], usd_balance[self.market_2] = usd_balance[self.market_1] - diff, usd_balance[
                self.market_2] + diff
        elif usd_balance[self.market_1] / usd_balance[self.market_2] < 0.75:
            diff = (usd_balance[self.market_2] - usd_balance[self.market_1]) / 2.0
            amount = round(diff / exchange_rate[self.market_2], trade_precision)
            tmp_symbol = '%s-%s' % (self.market_2, self.market_1)
            ticker = call_api_with_retry(self.client.get_order_book, tmp_symbol, limit=1)
            new_price = round(ticker['BUY'][0][0] * 1.0005, trade_precision)
            order_params = {'symbol': tmp_symbol, 'type': 'SELL', 'amount': amount, 'price': new_price}
            self.transaction_helper.create_and_track_single_order(order_params)
            usd_balance[self.market_1], usd_balance[self.market_2] = usd_balance[self.market_1] + diff, usd_balance[
                self.market_2] - diff

        log.info("Listing USD valued balances for all trading coins after filling gap.")
        log_json_utils(log.info, **usd_balance)

        # Update the balance_cache
        self.balance_cache = self.get_all_balances()

        log.info("Finished filling balance gap.")
        return

    def compare_market_balances(self, usd_balance):
        return (self.market_1, self.market_2) if usd_balance[self.market_1] < usd_balance[self.market_2] else (self.market_2, self.market_1)

    def detect_spread_in_all_coins(self):
        all_tickers = self.client.get_tick()
        ticker_cache = collections.defaultdict(list)
        for ticker in all_tickers:
            coin, coinPair = ticker[COIN_TYPE], ticker[COIN_TYPE_PAIR]
            tmp_symbol = '%s-%s' % (coin, coinPair)
            ticker_cache[coin].append({COIN_TYPE_PAIR: coinPair, TICKER: ticker})

        for ticker_group in ticker_cache.keys():
            for pair in itertools.combinations(ticker_cache[ticker_group], 2):
                market_1, market_2 = pair[0][COIN_TYPE_PAIR], pair[1][COIN_TYPE_PAIR]
                if (market_1, market_2) != ('BTC', 'ETH'):
                    continue
                base_ticker = list(filter(lambda x: x[COIN_TYPE_PAIR] == 'BTC', ticker_cache['ETH']))[0][TICKER]
                current_spread = self.get_spread_ticker_without_amount(pair[0][TICKER], pair[1][TICKER], base_ticker, ticker_group)
                if current_spread:
                    return self.detect_spread_and_fill(ticker_group, market_1, market_2)
        return False

    def get_spread_ticker_without_amount(self, ticker_1, ticker_2, base_ticker, coin):
        if any(i not in j or not j[i] for i in (BUY, SELL) for j in (ticker_1, ticker_2)):
            return
        ratio_12 = ticker_2[BUY] / ticker_1[SELL]
        ratio_21 = ticker_1[BUY] / ticker_2[SELL]
        spread_12 = ratio_12 * base_ticker[BUY]
        spread_21 = ratio_21 / base_ticker[SELL]
        if (spread_12 - 1) > 0.008:
            print("%s -> %s -> %s  " % (ticker_1[COIN_TYPE_PAIR], coin, ticker_2[COIN_TYPE_PAIR]),
                  "{:.2%}  ".format((spread_12 - 1) / 1.0), "unit %s" % ticker_1[COIN_TYPE_PAIR])
            log.info("%s -> %s -> %s  " % (ticker_1[COIN_TYPE_PAIR], coin, ticker_2[COIN_TYPE_PAIR]) + "{:.2%} ".format(
                (spread_12 - 1) / 1.0) + "unit: %s" % ticker_1[COIN_TYPE_PAIR])
            return (spread_12 - 1) / 1.0, True
        if (spread_21 - 1) > 0.008:
            print("%s -> %s -> %s  " % (ticker_2[COIN_TYPE_PAIR], coin, ticker_1[COIN_TYPE_PAIR]),
                  "{:.2%}  ".format((spread_21 - 1) / 1.0), "unit %s" % ticker_1[COIN_TYPE_PAIR])
            log.info("%s -> %s -> %s  " % (ticker_2[COIN_TYPE_PAIR], coin, ticker_1[COIN_TYPE_PAIR]) + "{:.2%} ".format(
                (spread_21 - 1) / 1.0) + "unit: %s" % ticker_1[COIN_TYPE_PAIR])
            return (spread_21 - 1) / 1.0, False
        return

    def get_min_order_amount(self):
        log.info("Getting minimum order amount data from configuration file.")
        abs_path = Path(Path(__file__).resolve().parents[1], COIN_METADATA_FILENAME)
        with abs_path.open('r') as f:
            return json.load(f)[MINIMUM_ORDER_AMOUNT]

    def get_orderbook_parallel(self, symbol_list):
        futures = [self.transaction_helper.executor.submit(self.get_order_book, symbol, limit=1) for symbol in
                   symbol_list]

        result = []
        try:
            for future in concurrent.futures.as_completed(futures, timeout=0.5):
                tmp_result = future.result()
                if tmp_result and 'symbol' in tmp_result:
                    result.append(tmp_result)
        except concurrent.futures._base.TimeoutError:
            log.warning("Didn't finish getting order books on time.")
            return None
        except Exception as e:
            log.error("Error while getting order book parallel.")
            log.error(e, exc_info=True)
            time.sleep(0.5)
            return None
        return result
