import urllib
import urllib2
import json
import os
import time
import hmac
import csv
import hashlib

#  Built 2/10/2016 by n-exploit
#  Poloniex API wrapper for Python built by @opiminer

api_key = '00000000-00000000-00000000-00000000'
secret = '0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000'

def clear():
    os.system('cls')
clear()


def createTimeStamp(datestr, dt_format="%Y-%m-%d %H:%M:%S"):
    return time.mktime(time.strptime(datestr, format))


class Poloniex:
    def __init__(self, APIKey, Secret):
        self.APIKey = APIKey
        self.Secret = Secret

    def post_process(self, before):
        after = before

        # Add timestamps if there isnt one but is a datetime
        if('return' in after):
            if(isinstance(after['return'], list)):
                for x in xrange(0, len(after['return'])):
                    if(isinstance(after['return'][x], dict)):
                        if('datetime' in after['return'][x] and 'timestamp' not in after['return'][x]):
                            after['return'][x]['timestamp'] = float(createTimeStamp(after['return'][x]['datetime']))

        return after

    def api_query(self, command, req={}):
        time.sleep(1)

        if(command == "returnTicker" or command == "return24Volume"):
            ret = urllib2.urlopen(urllib2.Request('https://poloniex.com/public?command=' + command))
            return json.loads(ret.read())
        elif(command == "returnOrderBook"):
            ret = urllib2.urlopen(urllib2.Request('https://poloniex.com/public?command=' + command + '&currencyPair=' + str(req['currencyPair'])))
            return json.loads(ret.read())
        elif(command == "returnMarketTradeHistory"):
            ret = urllib2.urlopen(urllib2.Request('https://poloniex.com/public?command=' + "returnTradeHistory" + '&currencyPair=' + str(req['currencyPair'])))
            return json.loads(ret.read())
        else:
            req['command'] = command
            req['nonce'] = int(time.time() * 1000)
            post_data = urllib.urlencode(req)

            sign = hmac.new(self.Secret, post_data, hashlib.sha512).hexdigest()
            headers = {
                'Sign': sign,
                'Key': self.APIKey
            }

            ret = urllib2.urlopen(urllib2.Request('https://poloniex.com/tradingApi', post_data, headers))
            jsonRet = json.loads(ret.read())
            return self.post_process(jsonRet)

    def returnTicker(self):
        return self.api_query("returnTicker")

    def returnMarketTradeHistory(self, currencyPair):
        return self.api_query("returnMarketTradeHistory", {'currencyPair': currencyPair})

    # Returns all of your balances.
    # Outputs:
    # {"BTC":"0.59098578","LTC":"3.31117268", ... }
    def returnBalances(self):
        return self.api_query('returnBalances')

    # Returns your trade history for a given market, specified by the "currencyPair" POST parameter
    # Inputs:
    # currencyPair  The currency pair e.g. "BTC_XCP"
    # Outputs:
    # date          Date in the form: "2014-02-19 03:44:59"
    # rate          Price the order is selling or buying at
    # amount        Quantity of order
    # total         Total value of order (price * quantity)
    # type          sell or buy
    def returnTradeHistory(self, currencyPair):
        return self.api_query('returnTradeHistory', {"currencyPair": currencyPair})


def update_balance():
    active_balance = {}
    total_balance = main.returnBalances()
    for b in total_balance:
        if float(total_balance[b]) > 0:
            active_balance[b] = float(total_balance[b])
    return(active_balance)


def update_value(currency_balance):
    current_prices = main.returnTicker()
    holdings = {}
    for cb in currency_balance:
        if cb == u'BTC':
            pass
        else:
            holdings[cb] = current_prices[u'BTC_' + cb]
    return holdings


class Portfolio:
    def __init__(self):
        self.currencies_owned = update_balance()
        self.ticker_data = update_value(self.currencies_owned)

main = Poloniex(api_key, secret)

global_trade_ids = set()
trades_by_ticker = {}

#  Loads market trade history from locally saved files. This files contain all
#  unique market trade history gathered during script execution
load_local = os.listdir(os.curdir)
for filename in load_local:
    if 'trade_history.csv' in filename:
        with open(filename) as csv_in:
            ticker = filename.split('_')[0]
            trades_by_ticker[ticker] = set()
            read_csv = csv.reader(csv_in)
            for row in read_csv:
                global_trade_id = int(row[-1])
                trades_by_ticker[ticker].add(global_trade_id)
                global_trade_ids.add(global_trade_id)

total_trades = len(global_trade_ids)
runs = 0
trades_since_start = 0

#  Gathers market trade history from your currently held balances and appends
#  this history to local files. Market trade history is gathered every 30
#  seconds. This script was designed to log market history for currencies with
#  lower volume. Future versions will make dynamic adjustments based on volume.
while True:
    try:
        pf = Portfolio()
    except KeyboardInterrupt:
        raise
    except:
        clear()
        print('Possible HTTP Error: Sleeping for 5 Minutes')
        time.sleep(300)
    runs += 1
    for ticker in pf.currencies_owned:
        if ticker not in trades_by_ticker:
            trades_by_ticker[ticker] = set()
        if ticker != 'BTC':
            ticker_history = main.returnMarketTradeHistory(u'BTC_' + ticker)
            with open('{}_trade_history.csv'.format(ticker), 'ab') as csv_out:
                write_csv = csv.writer(csv_out)
                for trade in ticker_history:
                        trade_id = trade['tradeID']
                        amount = trade['amount']
                        rate = trade['rate']
                        date = trade['date']
                        total = trade['total']
                        type_ = trade['type']
                        amount = trade['amount']
                        global_trade_id = trade['globalTradeID']
                        if global_trade_id not in global_trade_ids:
                            write_csv.writerow([trade_id, amount, rate, date, total,
                                                type_, amount, global_trade_id])
                            trades_by_ticker[ticker].add(global_trade_id)
                            global_trade_ids.add(global_trade_id)
                            total_trades += 1
                            trades_since_start += 1
                        else:
                            pass

    clear()
    c_owned = []
    runtime = 30
    t_per_min = float(float(trades_since_start) / float(runtime * 2))
    for currency in pf.currencies_owned:
        if currency not in c_owned and currency != 'BTC':
            c_owned.append(str(currency))

    print('''
POLONIEX Trade Aggregator
-------------------------
Runs: {}
Time Between Runs: {} seconds
Total Trades Logged: {}
Trades Since Start: {}
Trades Per Minute: {:.3f}
Currencies Owned: {}'''.format(runs, runtime, total_trades,
                               trades_since_start, t_per_min, c_owned))
    for ticker in trades_by_ticker:
        if ticker in pf.currencies_owned and ticker != 'BTC':
            ticker_trades = len(trades_by_ticker[ticker])
            print('''
    {}
    Total Trades: {}'''.format(ticker, ticker_trades))
    print('''
CTRL + C to QUIT''')
    time.sleep(runtime)
