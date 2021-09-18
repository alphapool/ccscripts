#!/usr/bin/python3

import psycopg2
import cv2
import requests
import numpy as np
import time
from datetime import datetime
import json

pause = 30

mikkas = ['vr', 'pizza_sh_sp', 'floor_r_sh_hd', 'floor_d_sh_sp', 'bed_lh_yk', 'bed_sh_sp', 'wine_sh_sp', 'pc']
mikkas = [[m, cv2.imread(m + '.png')] for m in mikkas]

def get_image(ipfs_hash):
    print(' Getting', ipfs_hash)
    res = requests.get('https://infura-ipfs.io/ipfs/' + unit['image'][7:])
    if res.status_code == 200:
        print(' done')
        return res.content
    else:
        time.sleep(1)
        return get_image(ipfs_hash)

while True:
    start = datetime.now()

    conn = psycopg2.connect(dbname='cardanocity', port=5432)
    cur = conn.cursor()
    cur.execute('select * from units where mikka is null')
    units = cur.fetchall()
    units = [unit[1] for unit in units]
    found = len(units)
    print('\n Found:', found)

    if found == 0:
        print(' Pausing for', pause)
        time.sleep(pause)
    else:
        n = 1
        positions = []
        for unit in units:
            print('\n', n, 'of', found, '- Getting image for', unit['name'])
            img = get_image(unit['image'][7:])
            img = cv2.imdecode(np.frombuffer(img, np.uint8), 1)
            vals = []
            for mikka in mikkas:
                try:
                    r = cv2.matchTemplate(mikka[1], img, cv2.TM_SQDIFF_NORMED)
                    min_val,_,_,_ = cv2.minMaxLoc(r)
                except:
                    min_val = 1
                vals.append([mikka[0], min_val])
                if min_val < 0.08:
                    break
            vals = sorted(vals, key=lambda k: k[1])
            positions.append([unit['name'], mikka[0]])
            print(positions[-1:])
            n += 1
        for p in positions:
            p[1] = {'position': p[1]}
            print(p)
            cur.execute('insert into units(name,mikka) values(\'' + p[0] + '\',\'' + json.dumps(p[1]).replace("\'", "\'\'") + '\') on conflict (name) do update set mikka=excluded.mikka')
        conn.commit()

    cur.close()
    conn.close()
    print(' Finished in', datetime.now() - start)
