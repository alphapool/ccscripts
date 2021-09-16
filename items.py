#!/usr/bin/python3

import psycopg2
import json

print('\n Getting assets...', end='')
conn = psycopg2.connect(dbname='cexplorer', port=5432)
cur = conn.cursor()
cur.execute("select block.time,tx_metadata.json from ma_tx_mint inner join tx_metadata on ma_tx_mint.tx_id = tx_metadata.tx_id inner join tx on ma_tx_mint.tx_id = tx.id inner join block on tx.block_id = block.id where ma_tx_mint.policy = '\\xa5425bd7bc4182325188af2340415827a73f845846c165d9e14c5aed'")
txs = cur.fetchall()
cur.close()
conn.close()
print('done\n')

txs = [[tx[0], tx[1]['a5425bd7bc4182325188af2340415827a73f845846c165d9e14c5aed']] for tx in txs]
assets = []

for tx in txs:
    for asset in tx[1]:
        tx[1][asset].update({'minted': tx[0].strftime("%Y-%m-%dT%H:%M:%S")})
        assets.append(tx[1][asset])

assets = { each['name'] : each for each in assets }.values()
assets = sorted(assets, key=lambda k: k['name'])
print(' Total assets:', len(assets))

print('\n Getting items...', end='')
items = []
for asset in assets:
    if asset['contents'] != []:
            for item in asset['contents']:
                    items.append(item)

items = { each['name'] : each for each in items }.values()
items = sorted(items, key=lambda k: int(k['instances']))
print('done\n')

print(' Total items:', len(items))

print('\n Inserting\n')
conn = psycopg2.connect(dbname='cardanocity', port=5432)
cur = conn.cursor()
for i, item in enumerate(items, 1):
    print('                                      ', end='\r')
    print('', item['name'], end='\r')
    cur.execute('insert into items(id,metadata) values(\'' + str(i) + '\',\'' + json.dumps(item).replace("\'", "\'\'") + '\')')
    conn.commit()
cur.close()
conn.close()
print('\n')
