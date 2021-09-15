#!/usr/bin/python3

import requests
import sys

url = ''

search_terms = sys.argv[1:-2]
max_results = int(sys.argv[-1:][0])
sort_by = sys.argv[-2:-1][0]

print('\n Getting assets...', end='')
res = requests.get(url)
if res.status_code == 200:
    assets = res.json()['assets']
    assets = [assets[x] for x in assets]
    print('done\n')
    print(' Total assets:', res.json()['total'], '\n')
else:
    print(' Failed:', res.status_code)

def search(search_terms, assets):
    results = []
    for asset in assets:
        if search_terms != []:
            if len(search_terms) == 1 and search_terms[0] == 'poster':
                if 'Poster' in asset['asset']['name']:
                    results.append(asset)
            elif len(search_terms) == 1 and search_terms[0] == 'top10':
                if 'CardanoCityUnit0000' in asset['asset']['name']:
                    results.append(asset)
            elif len(search_terms) == 1 and search_terms[0] == 'top100':
                if 'CardanoCityUnit000' in asset['asset']['name']:
                    results.append(asset)
            elif len(search_terms) == 1 and search_terms[0] == 'top1k':
                if 'CardanoCityUnit00' in asset['asset']['name']:
                    results.append(asset)
            elif len(search_terms) == 1 and search_terms[0] == 'top10k':
                if 'CardanoCityUnit0' in asset['asset']['name']:
                    results.append(asset)
            else:
                item_count = 0
                for search_term in search_terms:
                    if [item for item in asset['asset']['contents'] if search_term in item['name']] != []:
                        item_count += 1
                    if item_count == len(search_terms):
                        results.append(asset)
        else:
            results.append(asset)
    return results

def print_results(results, search_terms, max_results):
    print(' Found', len(results), search_terms, '\n')
    if len(search_terms) == 1 and results[0]['listing'] != None:
        price = results[0]['listing']['price'] / 1000000
        if search_terms[0] == 'top10':
                n_items = 10
        if search_terms[0] == 'top100':
                n_items = 100
        if search_terms[0] == 'top1k':
                n_items = 1000
        if search_terms[0] == 'top10k':
                n_items = 10000
        elif search_terms[0] == 'poster':
                n_items = 250
        else:
            for item in results[0]['asset']['contents']:
                if search_terms[0] in item['name']:
                    n_items = int(item['instances'])
        print('', n_items, search_terms[0], '*', price, '=', f'{n_items*price:,}', '\n')
    num_results = 0
    for result in results:
        name = result['asset']['name']
        print('', name, ' '*(24-len(name)), '- ', result['asset']['minted'], end='')
        if result['listing'] != None:
                price = result['listing']['price']/1000000
                print('  - ', price , ' '*(10-len(str(price))), '-  https://cnft.io/token.php?id=' + result['listing']['id'], end='')
        print('')
        num_results += 1
        if num_results >= max_results and max_results != 0:
            break

if sort_by == 'price':
    results = search(search_terms, assets)
    results = sorted(results, key=lambda k: k['asset']['name'])
    listings = []
    new_results = []
    for result in results:
        if result['listing'] != None:
            listings.append(result)
        else:
            new_results.append(result)
    listings = sorted(listings, key=lambda k: k['listing']['price'])
    new_results = sorted(new_results, key=lambda k: k['asset']['name'])
    listings = listings + new_results
    print_results(listings, search_terms, max_results)
    
elif sort_by == 'date':
    results = search(search_terms, assets)
    results = sorted(results, key=lambda k: k['asset']['minted'])
    print_results(results, search_terms, max_results)

elif sort_by == 'name':
    results = search(search_terms, assets)
    results = sorted(results, key=lambda k: k['asset']['name'])
    print_results(results, search_terms, max_results)

elif sort_by == 'floors':
    search_terms = [['samurai'], ['astro'], ['console'], ['painting'], ['mighty'], ['top1k'], ['poster']]
    for search_term in search_terms:
        results = search(search_term, assets)
        results = sorted(results, key=lambda k: k['asset']['name'])
        listings = []
        new_results = []
        for result in results:
            if result['listing'] != None:
                listings.append(result)
            else:
                new_results.append(result)
        listings = sorted(listings, key=lambda k: k['listing']['price'])
        new_results = sorted(new_results, key=lambda k: k['asset']['name'])
        listings = listings + new_results
        print_results(listings, search_term, max_results)
        print('\n')

else:
    pass

print('\n')
