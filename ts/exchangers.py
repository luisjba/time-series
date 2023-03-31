#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Crypto currency Exchangers
@datecreated: 2022-09-07
@lastupdated: 2022-09-07
@author: Jose Luis Bracamonte Amavizca
"""
# Meta information.
__author__ = 'Jose Luis Bracamonte Amavizca'
__version__ = '0.0.1'
__maintainer__ = 'Jose Luis Bracamonte Amavizca'
__email__ = 'luisjba@gmail.com'
__status__ = 'Development'

import os, sys
from sqlite3.dbapi2 import Row
from datetime import datetime
from binance import Client, ThreadedWebsocketManager, ThreadedDepthCacheManager
from .db import Connection
from .utils import print_fail, print_okgreen, print_okblue, print_warning

class Exchange:
    KLINE_INTERVAL_LIST = [
        Client.KLINE_INTERVAL_1MINUTE,
        Client.KLINE_INTERVAL_3MINUTE,
        Client.KLINE_INTERVAL_5MINUTE,
        Client.KLINE_INTERVAL_15MINUTE,
        Client.KLINE_INTERVAL_30MINUTE,
        Client.KLINE_INTERVAL_1HOUR,
        Client.KLINE_INTERVAL_2HOUR,
        Client.KLINE_INTERVAL_4HOUR,
        Client.KLINE_INTERVAL_6HOUR,
        Client.KLINE_INTERVAL_8HOUR,
        Client.KLINE_INTERVAL_12HOUR,
        Client.KLINE_INTERVAL_1DAY,
        Client.KLINE_INTERVAL_3DAY,
        Client.KLINE_INTERVAL_1WEEK,
        Client.KLINE_INTERVAL_1MONTH,
    ]
    CURRENCIES = {
        "BTC" : "Bitcoin",
        "ETH" : "Ethereum",
        "XRP" : "Ripple",
        "BNB" : "Binance",
        "ADA" : "Cardano",
        "SOL" : "Dolana",
        "DOGE" : "Degecoin",
        "DOT" : "Polkadot"
    }
    KLINE_INDEX_MAP = {
        "open_time":                        0,
        "open":                             1,
        "high":                             2,
        "low":                              3,
        "close":                            4,
        "volume":                           5,
        "close_time":                       6,
        "quote_asset_volume":               7,
        "trades_count":                     8,
        "taker_buy_base_asset_volume":      9,
        "taker_buy_quote_asset_volume":     10,
    }
    def __init__(self, api_key:str, api_secret:str, base_path:str="") -> None:
        """Constructor to create the client and db connection"""
        self._date_format:str = "%Y/%m/%d %H:%M:%S"
        self.client:Client = Client(api_key, api_secret)
        self.db_file:str = 'data/data.db'
        self.base_path:str = base_path
        self.db_conn:Connection = Connection(self.base_path, db_file)
        print_okgreen("SQLite connected to:{}".format(self.db_conn.db_file))
        self._init_db()

    def _init_db(self):
        """Function to initialize the database"""
        schemas_dir='schemas'
        schemas_dir = os.path.join(self.base_path, schemas_dir) if not schemas_dir[0] == "/" else schemas_dir
        if not os.path.isdir(schemas_dir):
            print_fail("'{}' is an invalid directory. Provide a valid directory to find the schemas for SQLite tables".format(schemas_dir))
            sys.exit(1)
        schemas:list = ['currency', 'currency_interval', 'klines']
        schame_dict = {schema:"{}/{}.sql".format(schemas_dir, schema) for schema in schemas}
        if self.db_conn is not None and len(schame_dict) > 0:
            for table_name,table_file  in schame_dict.items():
                if not os.path.isfile(table_file):
                    print_fail("Schema file '{}' does not exists".format(table_file))
                    sys.exit(1)
                    continue
                result = self.db_conn.execute_query_fetch('sqlite_master',['name'],{'type':'table', 'name':table_name})
                if len(result) > 0 : # The table already exists in the database
                    continue 
                with open(table_file, 'r') as file:
                    if self.db_conn.create_table(file.read()):
                        print_okgreen("Created the table {} into SQLite db: '{}'".format(table_name, self.db_conn.db_file))

    def sync_data(self,kline_intervals:list=[]) -> None:
        """Synchronize klines data from the Exchanger on the different intervals"""
        if len(kline_intervals) <= 0 :
            # fill with the default intervals
            kline_intervals = self.KLINE_INTERVAL_LIST
        for currency_name, description in self.CURRENCIES.items():
            currency_row = self.db_conn.get_currency(currency_name)
            if currency_row is None:
                currency_row = self.db_conn.add_currency(
                    {
                        "name":currency_name,
                        "description": description
                    }
                )
                if currency_row is None:
                    print_fail("Failed insert the currency row into SQLite db '{}'".format(currency_name))
                    sys.exit(1)
                print_okgreen("Currency '{}' successfully inserted into SQLite db".format(currency_name))
            
            # Sync Kline data the current currency in different intervals
            for interval_name in kline_intervals:
                if interval_name not in  self.KLINE_INTERVAL_LIST:
                    print_warning("The interval coe '{}' is invalid".format(interval_name))
                    continue
                currency_interval = self.db_conn.get_currency_interval(currency_row['id'], interval_name)
                if currency_interval is None:
                    currency_interval = self.db_conn.add_currency_interval(
                        {
                            "currency_id": currency_row['id'],
                            "name": interval_name,
                            "first_transaction_date":0,
                            "last_transaction_date":0
                        }
                    )
                    if currency_interval is None:
                        print_fail("Failed insert the currency interval row into SQLite db '{}'".format(interval_name))
                        sys.exit(1)
                    print_okgreen("Currency Interval '{}' successfully inserted into SQLite db".format(interval_name))
                # Klines sync
                kline_total = 0
                last_kline = None
                for kline in self.client.get_historical_klines(
                        symbol="{}USDT".format(currency_name), 
                        interval=interval_name, 
                        start_str=currency_interval["last_transaction_date"]
                    ):
                    kline_data = {
                        "currency_id" : currency_row['id'],
                        "currency_interval_id" : currency_interval['id'],
                        "interval_name": interval_name
                    }
                    for column, index in self.KLINE_INDEX_MAP.items():
                        kline_data[column] = kline[index]
                    # Format the date time
                    date_columns = ["open_time", "close_time"]
                    for c in date_columns:
                        kline_data[c] = round(kline_data[c]/1000)
                    # Store kline in the DB
                    kline_row = self.db_conn.add_kline(currency_row['id'], currency_interval['id'], kline_data)
                    if kline_row is None:
                        print_fail("Failed insert the kline '{}' into SQLite db".format(kline_data))
                        break
                    last_kline = kline_row
                    kline_total += 1
                if kline_total >= 1:
                    if last_kline is not None:
                        # Update Interval data
                        if currency_interval["first_transaction_date"] <= 0:
                            currency_interval["first_transaction_date"] = last_kline["open_time"]
                        currency_interval["last_transaction_date"] = last_kline["open_time"]
                        currency_interval = self.db_conn.update_currency_interval(currency_interval)
                        if currency_interval is None:
                            print_fail("Failed updating the kline interval '{}' into SQLite db".format(currency_interval))
                    print_okgreen("{} {} interval, {} records added synchronized.".format(currency_name, currency_interval["name"], kline_total))
                else:
                    print_okblue("{} {} interval is up to date.".format(currency_name, currency_interval["name"]))
            print_okblue("Finished synchronized {} intervals: ''.".format(currency_name, kline_intervals))
        print_okblue("Finished synchronized all currencies")

