import httplib
import threading
import time
import json
import sys
import random
import hmac
import hashlib
import urllib
import math
class LoadThread(threading.Thread):
    def __init__(self, market, sink):
        self._sink = sink
        self._market = market
        threading.Thread.__init__(self)

    def run(self):
        c = httplib.HTTPSConnection("btc-e.com")
        c.request("GET", "/api/2/%s_%s/depth" % self._market)
        response = c.getresponse()
        self._sink((self._market, response.read()))

def fetch_depth(markets):
    threads = []
    data = []
    for market in markets:
        thread = LoadThread(market, data.append)
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()
    return data

def collate_depth(data):
    result = {}
    for (a, b), response in data:
        try:
            response = json.loads(response)
        except Exception as e:
            print response
            raise e
        result[(a,b)] = [(float(price), float(n)) for price, n in response[u'bids']]
        result[(b,a)] = [(1.00/price, float(n)*price) for price, n in response[u'asks']]
    return result

def sell(ammount, pool):
    result = 0.0
    report = []
    for price, c in pool:
        if c >= ammount:
            result += price*ammount
            report.append((ammount, price))
            ammount = 0.0
            break
        result += price*c
        report.append((c, price))
        ammount -= c
    return result, ammount, report

def path_pairs(path):
    a = path[0]
    for e in path[1:]:
        yield a, e
        a = e

def clamp(x):
    return math.floor(x*10000.0)/10000.0
def run_path(path, initial, fee, markets):
    n = initial
    report = []
    for pair in path_pairs(path):
        n = clamp(n)
        min = 0.1 #0.01 if (pair[0] == 'btc' or pair[1] == 'btc') else 0.1
        if n < min:
            return n, report, True
        market = markets[pair]
        got, leftovers, sell_report = sell(n, market)
        got = clamp(got)
        n_2 = got * (1.00 - (0.005 if (pair == ('usd', 'rur') or pair == ('rur', 'usd')) else fee))
        report.append((pair, sell_report, n, got, got-n_2))
        n = n_2
        if n < min:
            return n, report, True
        if leftovers:
            raise Exception("leftovers %s/%s" % pair)
    return n, report, False

def expand(paths, pairs):
    result = []
    for path in paths:
        end = path[-1]
        for a, b in pairs:
            if a == end and b not in path[1:]:
                result.append(path + [b])
    return result

def cycles(symbols, pairs):
    paths = [[x] for x in symbols]
    result = []
    while paths:
        expanded = expand(paths, pairs)
        paths = []
        for path in expanded:
            if path[0] == path[-1]:
                if len(path) > 3:
                    result.append(path)
            else:
                paths.append(path)
    return result

def format_report(report):
    r = []
    for (a, b), bids, sold, got, fee in report:
        r.append(" %s/%s %f%s -> %f%s - %f%s" % (a, b, sold, a, got, b, fee, b))
        for amnt, price in bids:
            r.append("  %f %f" % (amnt, price))
    return "\n".join(r)

def nonce():
    nonce.i += 1
    return nonce.i
nonce.i = 400080

def main():
    pairs = [
        ('btc', 'usd'),
        ('btc', 'rur'),
        ('btc', 'eur'),
        ('ltc', 'usd'),
        ('ltc', 'rur'),
        ('ltc', 'btc'),
        ('usd', 'rur'),
        ('eur', 'usd'),
    ]
    symbol_weight = {
        'rur': 1.0/30,
        'usd': 30.0/30,
        'btc': 3500.0/30,
        'eur': 40.0/30,
        'ltc': 100.0/30,
    }
    fee = 0.002
    #symbols = 'usd rur eur ltc btc'.split()
    symbols = ['usd', 'ltc', 'rur']
    #variants = {'usd': [0.1, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5], 'ltc': [0.1, 0.5, 0.7], 'rur': range(10,25)}
    variants = {'usd': [0.1, 0.5]+range(1,10), 'ltc': [0.1, 0.5, 0.7], 'rur': range(10,25)}
    paths = cycles(symbols, pairs + [(b,a) for a, b in pairs])
    directions = dict(
        [((a,b), 'forward') for a, b in pairs] + [((b,a), 'backward') for a, b in pairs]
    )
    while True:
        t = time.time()
        print "Start", t,
        while True:
            try:
                data = fetch_depth(pairs)
                #data = json.loads(open("data.json", "r").read())
                depth = collate_depth(data)
                break
            except Exception as e:
                sys.stderr.write('Exception: %s\n' % (e,))
                time.sleep(300)

        print "Fetched", time.time()-t
        t = time.time()

        best = {}
        for path in paths:
            best_n, best_profit, _, _ = best.get(path[0], (0.0, 0.0, None, None))
            is_best = False
            is_profit = False
            for n in variants[path[0]]:
                n = float(n)
                result, report, trip = run_path(path, n, fee, depth)
                if (not trip) and (result - n > 0.0):
                    is_profit = True
                    if result - n > best_profit:
                        best_profit = result - n
                        best_report = report
                        best_n = n
                        is_best = True
            if is_profit:
                print '->'.join(path)
                print 'PROFIT: %d => %.4f%s' % (best_n, best_profit, path[0])
                if is_best:
                    best[path[0]] = best_n, best_profit, best_report, path
        executed = False
        for symbol in ['usd', 'ltc', 'rur']:
            if best.get(symbol):
                best_n, best_profit, best_report, best_path = best[symbol]
                print 'BEST FOR %s IS %.4f ON %.4f: %s' % (symbol, best_profit, best_n, '->'.join(best_path))
                print format_report(best_report)
                if not executed:
                    print 'running this one'
                    execute_path(best_report, directions)
                    executed = True
                    open("data.json", "w").write(json.dumps(data))
        if executed:
            return
        print "Done", time.time()-t
        sys.stdout.flush()

def execute_path(report, directions):
    for (a, b), bids, sold, got, fee in report:
        direction = directions[(a,b)]
        params = {
            "method": "Trade",
        }
        if direction == 'forward':
            params['pair'] = a + '_' + b
            params['type'] = 'sell'
            params['amount'] = '%.04f' % sold
            _, params['rate'] = bids[-1]
        else:
            params['pair'] = b + '_' + a
            params['type'] = 'buy'
            params['amount'] = '%.04f' % got
            _, params['rate'] = bids[-1]
            params['rate'] = 1.00/params['rate']
        result = call(params)

def call(params):
    key = 'KEY'
    secret = 'SECRET'
    params['nonce'] = nonce()
    qs = urllib.urlencode(params)

    H = hmac.new(secret, digestmod=hashlib.sha512)
    H.update(qs)
    sign = H.hexdigest()

    headers = {
        "Content-type": "application/x-www-form-urlencoded",
        "Key":key,
        "Sign":sign
    }
    t = time.time()
    print 'CALL', params
    conn = httplib.HTTPSConnection("btc-e.com")
    conn.request("POST", "/tapi", qs, headers)
    response = conn.getresponse()
    result = json.loads(response.read())
    print 'RESPONSE', result
    print 'T', time.time()-t
    return result

if __name__ == '__main__':
    main()
