import argparse
import threading
from pathlib import Path

from kucoin.client import Client

import secret_downloader
from helper import Helper
from utility import *
from constants import *

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)


class AccountManager(threading.Thread):
    def __init__(self, scan_coins, market_coins, semaphore):
        threading.Thread.__init__(self)
        self.helper = helper
        self.client = client
        self.coins = scan_coins
        self.market_coins = market_coins
        self.balance_cache = {}
        self.asset = 0.0
        self.initial_asset = None
        self.stop = False
        self.semaphore = semaphore
        self.trade_failure_count = 0

    def run(self):
        i = 0
        while not self.stop:
            if i % 3 == 0:
                log.info("Running account manager...")
                try:
                    self.asset = self.client.get_total_balance()
                except Exception as e:
                    log.error("Failed to get the account total balance. [Error thrown to the run method]")
                    log.error(e, exc_info=True)
                    continue
                if not self.initial_asset:
                    self.initial_asset = self.asset
                log.info("Current asset is %.2f" % self.asset)
            try:
                with self.semaphore:
                    self.helper.fill_balances_gap(self.coins)
            except Exception as e:
                log.error("Failed to fill the balances gap. [Error thrown to the run method]")
                log.error(e, exc_info=True)
            i += 1
            time.sleep(100)


class Trader(threading.Thread):
    def __init__(self, market_1, market_2, coins, semaphore):
        threading.Thread.__init__(self)
        self.helper = helper
        self.market_1, self.market_2 = market_1, market_2
        self.coins = coins
        self.stop = False
        self.profit = 0.0
        self.semaphore = semaphore

    def run(self):
        last_execution_millis = time.time()
        while not self.stop:
            i = 0
            while i < len(self.coins):
                coin = self.coins[i]
                log.info("Start scanning coin %s" % coin)
                try:
                    with self.semaphore:
                        if self.helper.detect_spread_and_fill(coin):
                            # Re-scan this coin in next loop to take more pending amount.
                            i -= 1
                except Exception as e:
                    # swallow any exception here.
                    log.error("Failed to detect spread and create orders for it. [Error thrown to the run method]")
                    log.error(e, exc_info=True)
                    time.sleep(1)
                if self.stop:
                    break
                time.sleep(0.02)
                i += 1
            if time.time() - last_execution_millis > 30:
                # Keep the http connection alive
                try:
                    self.helper.idle_executor()
                except Exception as e:
                    # swallow any exception here
                    log.error("Failed to do idle execution. [Error thrown to the run method]")
                    log.error(e, exc_info=True)
                    time.sleep(1)
                last_execution_millis = time.time()


class conrl(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.semaphore = semaphore
        self.thread_1 = AccountManager(scan_coins, markets, self.semaphore)
        self.thread_2 = Trader(markets[0], markets[1], scan_coins, self.semaphore)

    def run(self):
        print("program is executing...")
        self.thread_1.start()
        self.thread_2.start()
        i = 0
        while True:
            time.sleep(300)
            i += 1
            log.info("Round %d - Asset %.f - Profit %.8f" % (i, self.thread_1.asset, self.thread_2.profit))
            log_json_utils(log.info, message="Trade statics", data=self.thread_1.helper.trade_static)
            if self.thread_1.helper.transaction_helper.reaction_count > 0:
                average_reaction_time = self.thread_1.helper.transaction_helper.reaction_time / self.thread_1.helper.transaction_helper.reaction_count
                log.info("Trading reaction average time: %s ms", average_reaction_time * 1000)
            # if self.thread_2.profit < -10 or self.thread_2.profit > 100 or (
            if (self.thread_1.asset / self.thread_1.initial_asset) < 0.8:
                log.critical("Profit change reached threshold %.8f" % self.thread_2.profit)
                self.thread_2.stop = True
                break
        self.thread_1.stop = True
        self.thread_2.stop = True
        print("program is exiting...")
        time.sleep(2)
        exit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Find price gap within a platform")
    parser.add_argument('--coin', help='coin to be scanned, separated by dash(-).')
    parser.add_argument('--platform', help='Platform to trade.')
    parser.add_argument('--market', default='BTC-ETH', help='The two markets to trade against.')
    args = parser.parse_args()
    scan_coins = args.coin.split('-')

    # Get API credentials from AWS S3 bucket.
    abs_path = Path(Path(__file__).resolve().parents[1], CREDENTIALS_FILENAME)
    secret_downloader.download_secret(str(abs_path))
    with abs_path.open('r') as f:
        credentials = json.load(f)[args.platform]

    markets = args.market.split('-')

    client = Client(credentials['api_key'], credentials['secret_key'])
    helper = Helper(client, markets[0], markets[1])
    semaphore = threading.BoundedSemaphore(value=1)

    t3 = conrl()
    t3.start()
