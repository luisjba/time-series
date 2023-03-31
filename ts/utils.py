#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Utils classes
@datecreated: 2022-09-16
@lastupdated: 2022-12-01
@author: Jose Luis Bracamonte Amavizca
"""
# Meta informations.
__author__ = 'Jose Luis Bracamonte Amavizca'
__version__ = '0.0.1'
__maintainer__ = 'Jose Luis Bracamonte Amavizca'
__email__ = 'me@luisjba.com'
__status__ = 'Development'

import datetime

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def print_header(message:str):
    print("{}{}{}".format(bcolors.HEADER, message, bcolors.ENDC))

def print_okblue(message:str):
    print("{}{}{}".format(bcolors.OKBLUE, message, bcolors.ENDC))

def print_okcyan(message:str):
    print("{}{}{}".format(bcolors.OKCYAN, message, bcolors.ENDC))

def print_okgreen(message:str):
    print("{}{}{}".format(bcolors.OKGREEN, message, bcolors.ENDC))

def print_warning(message:str):
    print("{}{}{}".format(bcolors.WARNING, message, bcolors.ENDC))

def print_fail(message:str):
    print("{}{}{}".format(bcolors.FAIL, message, bcolors.ENDC))

def print_bold(message:str):
    print("{}{}{}".format(bcolors.BOLD, message, bcolors.ENDC))

def print_underline(message:str):
    print("{}{}{}".format(bcolors.UNDERLINE, message, bcolors.ENDC))

def timestamp2datetime(time_stamp:int, tz_hour_dif:int=-7) -> datetime.datetime:
    """Converts timestamp to datetime using a Time Zone"""
    dt = datetime.datetime.fromtimestamp(time_stamp/1000, datetime.timezone.utc)
    delta_hour = datetime.timedelta(hours=abs(tz_hour_dif))
    if tz_hour_dif == 0:
        return dt
    elif tz_hour_dif < 0:
        return dt - delta_hour
    else:
        return dt + delta_hour