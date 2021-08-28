# -*- coding: utf-8 -*-
"""
Created on Thu Aug 26 19:11:11 2021

@author: aleks1212
"""
from flask import Flask, request
from apscheduler.schedulers.background import BackgroundScheduler

import re
import pandas as pd
import os
from bs4 import BeautifulSoup
import requests
import json

import sqlite3
from datetime import datetime

currdir = os.getcwd()
dbid = os.path.join(currdir, r"log.db")

def parser(house):
    main_href = 'https://novosibirsk.n1.ru/'
    param_list = []
    req = requests.get(house)
    print(req.ok)
    
    soup = BeautifulSoup(req.text, "html.parser")
    data = soup.html.body
    #print(data)
    try:
        links = []
        main_link = data.findAll("div", {"class": "card-title living-list-card__inner-block"})
        for link in main_link:
            href = re.search(r'href="\S+"', str(link))
            href = href.group()[6:-1]
            links.append(main_href + href)
        print(len(links))
        
        for link in links:
            req = requests.get(link)
            soup = BeautifulSoup(req.text, "html.parser")
            soup_ = soup.html.body
            data = soup_.find("div", {"class": "card-living-content__params"})
            
            key_values = data.findAll("li", {"class":"card-living-content-params-list__item"})
            keys = []
            values = []
            for key_val in key_values:
                key = key_val.find("span", {"class":"card-living-content-params-list__name"}).text
                value = key_val.find("span", {"class":"card-living-content-params-list__value"}).text
                value = value.replace('\xa0м2', '')
                value = value.replace(',', '.')
                keys.append(key)
                values.append(value)
                
            param = ['Общая площадь', 'Год постройки', 'Этаж', 'Материал дома']
            param_val = [] #значения
            
            for p in param:
                if p in keys:
                    param_val.append(values[keys.index(p)])
                else:
                    param_val.append(None)
    
            address = data.find("span", {"class":"ui-kit-link__inner"}).text
            address = address.replace(' стр.', '')
            param_val.append(address)
            
            data_price = soup_.find("span", {"class": "price"}).text
            data_price = re.sub('[^\d\.]', '', data_price)
            param_val.append(data_price)
            
            scripts = soup_.findAll('script')
            script = None
            for s in scripts:
                if len(re.findall('__INITIAL_STATE__', s.get_text())) > 0:
                    script = s
                    break
            if script != None:
                script = script.get_text()
                script = re.sub(r';var pageMeta =.+','', script)
                num = script.find(r'{"contexts"')
                script = script[num:]
                script = script.replace('undefined','false')
                script = re.sub(r'new Date\(".+"\)', 'false', script)
                dataform = script #str(script).strip("'<>() ").replace('\'', '\"')
                json_s = json.loads(dataform)
                latitude = json_s['__INITIAL_STATE__']['OfferCard']['Location']['offerLocation']['latitude']
                longtitude = json_s['__INITIAL_STATE__']['OfferCard']['Location']['offerLocation']['longtitude']
                param_val.append(latitude)
                param_val.append(longtitude)
            print(param_val)
            param_list.append(param_val)
    except Exception as e:
        print('Error occured: ', e)
    return(param_list)

#каждый день запуск
def run_parser():
    
    houses = ['https://novosibirsk.n1.ru/search/?rubric=flats&deal_type=sell&limit=100&addresses%5B0%5D%5Bstreet_id%5D=865516&addresses%5B0%5D%5Bhouse_number%5D=252%2F1',
                   'https://novosibirsk.n1.ru/search/?addresses%5B0%5D%5Bstreet_id%5D=864462&addresses%5B0%5D%5Bhouse_number%5D=23%2F2%20%D1%81%D1%82%D1%80.&deal_type=sell&rubric=flats&limit=100',
                   'https://novosibirsk.n1.ru/search/?addresses%5B0%5D%5Bstreet_id%5D=864657&addresses%5B0%5D%5Bhouse_number%5D=274%20%D1%81%D1%82%D1%80.&deal_type=sell&rubric=flats&limit=100',
                   'https://novosibirsk.n1.ru/search/?addresses%5B0%5D%5Bstreet_id%5D=865373&addresses%5B0%5D%5Bhouse_number%5D=96%2F3&deal_type=sell&rubric=flats&limit=100',
                   'https://novosibirsk.n1.ru/search/?addresses%5B0%5D%5Bstreet_id%5D=864333&addresses%5B0%5D%5Bhouse_number%5D=209&deal_type=sell&rubric=flats&limit=100',
                   'https://novosibirsk.n1.ru/search/?addresses%5B0%5D%5Bstreet_id%5D=864940&addresses%5B0%5D%5Bhouse_number%5D=167%2F1&deal_type=sell&rubric=flats&limit=100',
                   'https://novosibirsk.n1.ru/search/?addresses%5B0%5D%5Bstreet_id%5D=864974&addresses%5B0%5D%5Bhouse_number%5D=82&deal_type=sell&rubric=flats&limit=100',
                   'https://novosibirsk.n1.ru/search/?addresses%5B0%5D%5Bstreet_id%5D=864137&addresses%5B0%5D%5Bhouse_number%5D=32%2F1%20%D1%81%D1%82%D1%80.&deal_type=sell&rubric=flats&limit=100',
                   'https://novosibirsk.n1.ru/search/?addresses%5B0%5D%5Bstreet_id%5D=864061&addresses%5B0%5D%5Bhouse_number%5D=60%20%D1%81%D1%82%D1%80.&deal_type=sell&rubric=flats&limit=100',
                   'https://novosibirsk.n1.ru/search/?addresses%5B0%5D%5Bstreet_id%5D=865274&addresses%5B0%5D%5Bhouse_number%5D=77&deal_type=sell&rubric=flats&limit=100',
                   ]
    n_houses = len(houses)
    for i in range(n_houses):
        param_list = parser(houses[i])
        store_message(param_list, dbid)
    return

sched = BackgroundScheduler(daemon=True)
sched.add_job(run_parser,'interval',minutes=10)
sched.start()

app = Flask(__name__)



def create_table_if_not_exists(conn, cursor):
    #cursor.execute("""DROP TABLE IF EXISTS mytable""")
    #conn.commit()
    cursor.execute("""CREATE TABLE IF NOT EXISTS mytable(
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
        area INTEGER NOT NULL,
        year VARCHAR(10),
        floor VARCHAR(10),
        material VARCHAR(10),
        address VARCHAR(100) NOT NULL,
        price INTEGER NOT NULL,
        latitude VARCHAR(10),
        longtitude VARCHAR(10),
        datetime DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL)""")
    conn.commit()
    return

def store_message(param_list, dbid):
    try: 
        #подключение базы данных
        conn = sqlite3.connect(dbid) # или :memory: чтобы сохранить в RAM
        with conn:
            cursor = conn.cursor()
            create_table_if_not_exists(conn, cursor)
            query = '''insert into mytable (area, year, floor, \
            material, address, price, latitude, longtitude) values (?, ?, ?, ?, ?, ?, ?, ?)'''
            for param in param_list:
                conn.execute(query, param)
                conn.commit()
    except sqlite3.IntegrityError as e:
        print('Error occured: ', e)
    finally:
        if conn:
            conn.close()
    return True

def get_message(limit, dbid, columns = None):
    #подключение базы данных
    try:
        conn = sqlite3.connect(dbid) # или :memory: чтобы сохранить в RAM
        with conn:
            cursor = conn.cursor()
            create_table_if_not_exists(conn, cursor)
            if columns == None:
                query = 'SELECT DISTINCT address FROM mytable ORDER BY datetime LIMIT ?'
                param_list = [limit]
            else:
                query = 'SELECT area, price, datetime FROM mytable WHERE address = ? LIMIT ?'
                param_list = [columns, limit]
            cursor.execute(query, param_list)
            conn.commit()
            rows = cursor.fetchall()
    except Exception:
        print('Ошибка обработки')
    finally:
        if conn:
            conn.close()
    return rows


@app.route('/', methods=['POST'])     
def mean_plot():        
    addr_ = get_message(-1, dbid)
    addr = []
    for a in addr_:
        temp = a[0].replace(' стр.', '')
        if temp not in addr:
            addr.append(temp)
    
    addr_list = []
    price_list = []
    
    for a in addr:
        print(a)
        mean_price_list = get_message(-1, dbid, a)
        df = pd.DataFrame(mean_price_list, columns = ['area', 'price', 'datetime'])
        df['price'] /= df['area']
        df['datetime'] = pd.to_datetime(df['datetime'])
        day = []
        for i in range(df.shape[0]):
            day.append(datetime.strftime(df.datetime[i], '%d.%m.%y %H h'))
        df.datetime = day
        df = df.sort_values("datetime", ascending=False)
        df = df.groupby(by = ['datetime']).mean()
        addr_list.append(a)
        price_list.append(df['price'].to_dict())

    return json.dumps({'addr':addr_list, 'price':price_list}).encode('utf-8')
        
        

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
    

    
    