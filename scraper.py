#!/usr/bin/python3

import requests
import psycopg2
import json
from datetime import datetime

def get_listings(page, trys):
    payload = {'search': 'CardanoCity', 'sort': 'price', 'order': 'asc', 'page': page, 'verified': 'true'}
    res = requests.post('https://api.cnft.io/market/listings', data=payload)
    if res.status_code != 200 and trys < 3:
        trys += 1
        return get_listings(page, trys)
    else:
        return res

while True:
    start = datetime.now()

    conn = psycopg2.connect(dbname='cexplorer', port=5432)
    cur = conn.cursor()
    cur.execute("select block.time,tx_metadata.json from ma_tx_mint inner join tx_metadata on ma_tx_mint.tx_id = tx_metadata.tx_id inner join tx on ma_tx_mint.tx_id = tx.id inner join block on tx.block_id = block.id where ma_tx_mint.policy = '\\xa5425bd7bc4182325188af2340415827a73f845846c165d9e14c5aed'")
    txs = cur.fetchall()
    cur.close()
    conn.close()

    txs = [[tx[0], tx[1]['a5425bd7bc4182325188af2340415827a73f845846c165d9e14c5aed']] for tx in txs]
    assets = []

    for tx in txs:
        for asset in tx[1]:
            tx[1][asset].update({'minted': tx[0].strftime("%Y-%m-%dT%H:%M:%S")})
            assets.append(tx[1][asset])

    assets = { each['name'] : each for each in assets }.values()
    assets = sorted(assets, key=lambda k: k['name'])
    print('\n Total assets:', len(assets))

    print('\n Pulling listings\n')

    page = 1
    listings = []
    while True:
        print(' Page', page, end='\r')
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
        page += 1

    listings = { each['metadata']['name'] : each for each in listings }.values()
    print('\n\n Total listings:', len(listings))
    print('\n Combining lists \n')

    a_l_combined = []
    for asset in assets:
        print('                         ', end='\r')
        print(' ' + asset['name'], end='\r')
        a_l_combined.append([asset, [{'name': listing['metadata']['name'], 'id': listing['id'], 'sold': listing['sold'], 'price': listing['price']} for listing in listings if listing['metadata']['name'] == asset['name']]])

    print('\n\n Inserting assets\n')

    conn = psycopg2.connect(dbname='cnft', port=5432)
    cur = conn.cursor()

    for listing in a_l_combined:
        print('                         ', end='\r')
        print(' ' + listing[0]['name'], end='\r')
        if listing[1] != []:
            cur.execute('insert into cnftio(name,metadata,listing) values(\'' + listing[0]['name'] + '\',\'' + json.dumps(listing[0]).replace("\'", "\'\'") + '\',\'' + json.dumps(listing[1][0]).replace("\'", "\'\'") + '\') on conflict (name) do update set metadata=excluded.metadata,listing=excluded.listing')
        else:
            cur.execute('insert into cnftio(name,metadata,listing) values(\'' + listing[0]['name'] + '\',\'' + json.dumps(listing[0]).replace("\'", "\'\'") +  '\',NULL) on conflict (name) do update set metadata=excluded.metadata,listing=excluded.listing')
        conn.commit()

    cur.close()
    conn.close()
    finish = datetime.now()

    print('\n\n Finished in:', finish - start, '\n')
