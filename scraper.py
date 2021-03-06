#!/usr/bin/python3

import requests
import threading
import math
import time
import psycopg2
import json
from datetime import datetime

def get_listings(page, trys):
    payload = {'search': 'CardanoCity', 'sort': 'price', 'order': 'asc', 'page': page, 'verified': 'true'}
    try:
        res = requests.post('https://api.cnft.io/market/listings', data=payload)
        if res.status_code != 200 and trys < 3:
            trys += 1
            return get_listings(page, trys)
        else:
            return res
    except:
        return get_listings(page, trys)

def scrape(pages, listings):
    for page in pages:
        res = get_listings(page, 0)
        if res.status_code == 200:
            if res.json()['assets'] == []:
                break
            else:
                try:
                    for listing in res.json()['assets']:
                        listings.append(listing)
                except:
                    pass

print('\n  Getting items...', end='')
conn = psycopg2.connect(dbname='cardanocity', port=5432)
cur = conn.cursor()
cur.execute("select * from items")
items = cur.fetchall()
cur.close()
conn.close()
print('done\n')

while True:
    start = datetime.now()

    print('  Getting assets...', end='')
    try:
        conn = psycopg2.connect(dbname='cexplorer', port=5432)
        cur = conn.cursor()
        cur.execute("select block.time,tx_metadata.json from ma_tx_mint inner join tx_metadata on ma_tx_mint.tx_id = tx_metadata.tx_id inner join tx on ma_tx_mint.tx_id = tx.id inner join block on tx.block_id = block.id where ma_tx_mint.policy = '\\xa5425bd7bc4182325188af2340415827a73f845846c165d9e14c5aed'")
        txs = cur.fetchall()
        cur.close()
        conn.close()
        print('done\n')

        txs = [[tx[0], tx[1]['a5425bd7bc4182325188af2340415827a73f845846c165d9e14c5aed']] for tx in txs]
        assets = []

        remove = ['files', 'mediaType', 'description', 'numberOfItems']
        for tx in txs:
            for asset in tx[1]:
                tx[1][asset].update({'minted': tx[0].strftime("%Y-%m-%dT%H:%M:%S")})
                for key in remove:
                    del tx[1][asset][key]
                assets.append(tx[1][asset])

        assets = { each['name'] : each for each in assets }.values()
        assets = sorted(assets, key=lambda k: k['name'])
        print('  Total assets:', len(assets))
    except:
        pass

    print('\n  Debloating contents...', end='')
    try:
        for asset in assets:
            item_list = []
            for content in asset['contents']:
                item_list.append([item[0] for item in items if item[1]['name'] == content['name']][0])
            asset['contents'] = item_list
    except:
        pass

    print('done\n\n  Pulling listings\n')

    try:
        found = get_listings(1, 0).json()['found']
        print(f"  Found: {found}\n")

        threads_n = 8

        s = math.ceil(found/(threads_n*24))

        listings = []
        threads = []
        for i in range(threads_n):
            listings.append([])
            threads.append(threading.Thread(target=scrape, args=(list(range(i*s+1,(i+1)*s+1)),listings[i],)))
            threads[i].start()

        while True:
            stopped = 0
            for t in threads:
                if not t.is_alive():
                    stopped += 1
            if stopped == len(threads):
                listings = [listing for sublist in listings for listing in sublist]
                break
            else:
                n = 0
                for l in listings:
                    n += len(l)
                print(f"  Listings: {n}", end='\r')
            time.sleep(1)
        print('\n\n  Total listings:', len(listings))
    except:
        pass

    print('\n  Combining lists\n')

    try:
        a_l_combined = []
        for asset in assets:
            print('                          ', end='\r')
            print('  ' + asset['name'], end='\r')
            a_l_combined.append([asset, [{'name': listing['metadata']['name'], 'id': listing['id'], 'sold': listing['sold'], 'price': listing['price']} for listing in listings if listing['metadata']['name'] == asset['name']]])
    except:
        pass

    print('\n\n  Inserting assets\n')
    try:
        conn = psycopg2.connect(dbname='cardanocity', port=5432)
        cur = conn.cursor()
        for asset in a_l_combined:
            print('                          ', end='\r')
            print('  ' + asset[0]['name'], end='\r')
            if asset[1] != []:
                if 'Unit' in asset[0]['name']:
                    cur.execute('insert into units(name,metadata,listing) values(\'' + asset[0]['name'] + '\',\'' + json.dumps(asset[0]).replace("\'", "\'\'") + '\',\'' + json.dumps(asset[1][0]).replace("\'", "\'\'") + '\') on conflict (name) do update set metadata=excluded.metadata,listing=excluded.listing')
                if 'Poster' in asset[0]['name']:
                    cur.execute('insert into posters(name,metadata,listing) values(\'' + asset[0]['name'] + '\',\'' + json.dumps(asset[0]).replace("\'", "\'\'") + '\',\'' + json.dumps(asset[1][0]).replace("\'", "\'\'") + '\') on conflict (name) do update set metadata=excluded.metadata,listing=excluded.listing')
            else:
                if 'Unit' in asset[0]['name']:
                    cur.execute('insert into units(name,metadata,listing) values(\'' + asset[0]['name'] + '\',\'' + json.dumps(asset[0]).replace("\'", "\'\'") +  '\',NULL) on conflict (name) do update set metadata=excluded.metadata,listing=excluded.listing')
                if 'Poster' in asset[0]['name']:
                    cur.execute('insert into posters(name,metadata,listing) values(\'' + asset[0]['name'] + '\',\'' + json.dumps(asset[0]).replace("\'", "\'\'") +  '\',NULL) on conflict (name) do update set metadata=excluded.metadata,listing=excluded.listing')
            conn.commit()
        cur.close()
        conn.close()
    except:
        pass

    finish = datetime.now()
    print('\n\n  Finished in:', finish - start, '\n')
