import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import os
import time
from pymongo import MongoClient
import urllib.parse
import redis

def find_hashes():
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)

    url = "https://www.blockchain.com/btc/unconfirmed-transactions"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    unconfirmed_tran = soup.find_all('div', {"class":'sc-1g6z4xm-0 hXyplo'})

    Hasherlist = list()
    Timelist = list()
    BTClist = list()
    USDlist = list()

    for tran in unconfirmed_tran:
        hasher = tran.find('a', {"class":"sc-1r996ns-0 fLwyDF sc-1tbyx6t-1 kCGMTY iklhnl-0 eEewhk d53qjk-0 ctEFcK"}).text
        time = tran.find('span', {"class":"sc-1ryi78w-0 cILyoi sc-16b9dsl-1 ZwupP u3ufsr-0 eQTRKC"}).text
        amounts = tran.find_all('div', {"class":"sc-1au2w4e-0 fTyXWG"})
        Hasherlist.append(hasher)
        Timelist.append(time)
        BTClist.append(amounts[0].text)
        USDlist.append(amounts[1].text)       

    sBTClist = [s.strip("Amount (BTC)") for s in BTClist]
    sUSDlist = [s.strip("Amount (USD)") for s in USDlist]

    df_hashes = pd.DataFrame(list(zip(Hasherlist, Timelist, sBTClist, sUSDlist)),
                   columns =['Hash', 'Time', 'Amount(BTC)', 'Amount(USD)'])

    df_hashes_sorted = df_hashes.sort_values(by=['Amount(BTC)'], ascending=False)

    hash_values = df_hashes_sorted.iloc[:1].to_string(index = False, header=False)
    
    shash_values = hash_values.strip()
    
    shash_valuesli = list(shash_values.split(" "))
    
    shash_valuesdict = {'Hash':shash_valuesli[0], 'Time':shash_valuesli[1], 'Amount(BTC)':shash_valuesli[2], 'Amount(USD)':shash_valuesli[3]}

    return shash_valuesdict

redis_db = redis.Redis(decode_responses=True)

client = MongoClient("mongodb://127.0.0.1:27017")
hashes_db = client["hashesdb"]
hashes_col = hashes_db["hashescol"]

while(True):
    hashtransaction = find_hashes()
    redis_db.hset("hashtransactionkey", mapping=hashtransaction)
    redis_db.expire("hashtransactionkey", 60)
    redis_db_getter = redis_db.hgetall('hashtransactionkey')
    x = hashes_col.insert_one(redis_db_getter)
    time.sleep(60)
