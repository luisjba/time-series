#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Crossover trading strategy implementation

@datecreated: 2022-09-06
@lastupdated: 2022-09-07
@author: Jose Luis Bracamonte Amavizca
"""
# Meta information.
__author__ = 'Jose Luis Bracamonte Amavizca'
__version__ = '0.0.1'
__maintainer__ = 'Jose Luis Bracamonte Amavizca'
__email__ = 'luisjba@gmail.com'
__status__ = 'Development'

import pandas as pd

BINANCE_COLUMNS = ["open_time","open","high","low","close","volume","close_time","quote_asset_volume","trades_count",
"taker_buy_base_asset_volume","taker_buy_quote_asset_volume","unused"]

def binanceData2DataFrame(klines_list:list) -> pd.DataFrame:
    """Convert Binance klines data to pandas DataFrame"""
    df = pd.DataFrame(klines_list, columns=BINANCE_COLUMNS)
    # formatting dates columns
    date_columns = ["open_time", "close_time"]
    for c in date_columns:
        df[c] = pd.to_datetime(df[c]/1000, unit="s")
    # formatting numeric columns
    numeric_columns = [c for c  in BINANCE_COLUMNS if c not in date_columns]
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric)
    # set the open_time as index for the time series
    df.set_index('open_time', inplace=True)
    return df



