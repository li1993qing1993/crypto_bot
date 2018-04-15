import concurrent.futures

from kucoin.exceptions import *
from requests.adapters import HTTPAdapter

from utility import *

log = logging.getLogger(__name__)


class TransactionHelper(object):
    MAX_RETRY = 3

    def __init__(self, client):
        self.client = client
        self.add_http_connection_pool()
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=3)
        self.reaction_count = 0
        self.reaction_time = 0.0

    def add_http_connection_pool(self):
        self.client.session.mount('http://', HTTPAdapter(pool_connections=1, pool_maxsize=3))

    def list_active_orders(self, symbol):
        response = call_api_with_retry(self.client.get_active_orders, symbol)
        # Merge two lists
        return response['BUY'] + response['SELL']

    def match_dealt_orders(self, order_params):
        dealt_orders = call_api_with_retry(self.client.get_symbol_dealt_orders,
                                           order_params['symbol'], order_params['type'], 6)['datas']
        for order in dealt_orders:
            if order['direction'] == order_params['type'] and time.time() * 1000 - float(order['createdAt']) < 200000:
                return order['orderOid'], order['amount']

    def check_order_dealt_amount(self, order_id, order_params):
        dealt_orders = call_api_with_retry(self.client.get_symbol_dealt_orders,
                                            order_params['symbol'], order_params['type'], 10)['datas']
        deal_total = 0.0
        for order in dealt_orders:
            if order_id == order['orderOid']:
                deal_total += float(order['amount'])
        return order_id, deal_total

    def match_active_order(self, order_params):
        order_book = self.list_active_orders(order_params['symbol'])
        for order in order_book:
            if order[1] == order_params['type'] and order[2] - order_params['price'] < 1e-9 and order[3] - order_params[
                'amount'] < 1e-9:
                return order[5]

    def create_order(self, order_params):
        log_json_utils(log.info, message="Creating an order.", symbol=order_params['symbol'], type=order_params['type'],
                       price=str(order_params['price']), amount=str(order_params['amount']))
        try:
            response = self.client.create_order(order_params['symbol'],
                                                order_params['type'], str(order_params['price']),
                                                str(order_params['amount']))
            if not response or "orderOid" not in response:
                raise TypeError("None or invalid response type.")
            else:
                return response["orderOid"], 0.0
        except (KucoinAPIException, KucoinRequestException, TypeError) as e:
            log.error('Error while creating an order for %s' % order_params['symbol'].split('-')[0])
            logging.error(e, exc_info=True)

        log.error('Checking active orders and dealt orders to locate the order.')
        # Search for orderOid in active order and dealt orders
        for i in range(4):
            orderOid = self.match_active_order(order_params)
            if orderOid:
                return orderOid, 0.0
            else:
                dealt_orderOid, deal_amount = self.match_dealt_orders(order_params)
                if dealt_orderOid:
                    return dealt_orderOid, deal_amount
            exponential_delay(0.2, i)
        raise Exception("Order creation failed, cannot locate the orderOid.")

    def cancel_active_order(self, orderOid, order_type, symbol):
        call_api_with_retry(self.client.cancel_order, orderOid, order_type, symbol=symbol)

    def deal_sequential_orders(self, order_params_group):
        """
        Create orders sequentially. Order creation depends on the completion of the previous order.
        
        :param order_params_group: Group of order_params
        :return: 
        """
        count = 0
        for order_params in order_params_group:
            orderOid, deal_amount = self.create_order(order_params)
            if deal_amount:
                continue
            found = False
            for i in range(10):
                if orderOid == self.match_dealt_orders(order_params):
                    count += 1
                    found = True
                    break
                elif orderOid == self.match_active_order(order_params):
                    time.sleep(0.1 * i)
                else:
                    # Move on to next order.
                    found = True
                    break
            if found:
                continue
            self.cancel_active_order(orderOid, order_params['type'], order_params['symbol'])
            break
        if count == len(order_params_group):
            log.info("Successfully created and dealt all orders.")
        else:
            log.error("Failed: Only successfully created and dealt %d orders.", count)
            raise Exception("Failed to create and deal all requested orders")

    def create_and_track_single_order(self, order_params, additional_wait_time=0):
        start_time = time.time()
        orderOid, deal_amount = self.create_order(order_params)
        log_json_utils(log.info, message="Successfully created an order.", orderOid=orderOid,
                       symbol=order_params['symbol'],
                       type=order_params['type'])
        self.reaction_count += 1
        self.reaction_time += time.time() - start_time
        if abs(order_params['amount'] - deal_amount) < 1e-9:
            log.info("Order %s has been fully dealt.", orderOid)
            return 1.0
        else:
            for i in range(10 + additional_wait_time):
                tmp_order_id, deal_amount = self.check_order_dealt_amount(orderOid, order_params)
                deal_ratio = deal_amount / order_params['amount']
                if orderOid == tmp_order_id and deal_ratio > 0.999:
                    log.info("Order %s has been fully dealt.", orderOid)
                    return 1.0
                time.sleep(0.8 * i)
            log_json_utils(log.error, message="The transaction didn't finished on time. Cancelling...",
                           orderOid=orderOid, symbol=order_params['symbol'], type=order_params['type'],
                           amount=order_params['amount'], deal_amount=deal_amount)
            self.cancel_active_order(orderOid, order_params['type'], order_params['symbol'])
            log.error("Successfully cancelled order %s", orderOid)
            return deal_ratio

    def deal_parallel_orders(self, order_params_group, timeout=60, additional_wait_time=0):
        """
        Create orders in parallel.
        :return: ratio of dealt quantity against submitted quantity
        """
        futures = [self.executor.submit(self.create_and_track_single_order, order_params, additional_wait_time) for order_params in
                   order_params_group]
        min_deal_ratio = 1.0
        for future in concurrent.futures.as_completed(futures, timeout=timeout):
            try:
                deal_ratio = future.result()
                min_deal_ratio = min(min_deal_ratio, deal_ratio)
                if abs(1.0 - deal_ratio) >= 0.02:
                    self.trade_on_fail(order_params_group)
            except Exception as e:
                log.error("Error while deal orders parallel.")
                log.error(e, exc_info=True)
                self.trade_on_fail(order_params_group)
                min_deal_ratio = 0.0
        return min_deal_ratio

    def idle_all_executors(self):
        """
        The website may periodically kill the session. Call API periodically to keep all connections in the pool alive.
        :return:
        """
        log.info("Idle executors.")
        futures = [self.executor.submit(self.list_active_orders, symbol) for symbol
                   in ["ETH-BTC", "VEN-BTC", "VEN-ETH"]]
        for future in concurrent.futures.as_completed(futures, timeout=10):
            try:
                future.result()
            except Exception as e:
                log.error("Error while keeping executors alive.")
                log.error(e, exc_info=True)

    def trade_on_success(self, symbols):
        pass

    def trade_on_fail(self, order_params_group):
        log.info("Clean-up, cancelling all active orders.")
        try:
            for order_params in order_params_group:
                call_api_with_retry(self.client.cancel_all_orders, symbol=order_params['symbol'])
        except Exception as e:
            log.error("Failed to cancel all active orders.")
            log.error(e, exc_info=True)
