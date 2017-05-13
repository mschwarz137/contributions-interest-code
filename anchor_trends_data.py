# -*- coding: utf-8 -*-
"""
Created on Thu Apr 20 01:28:56 2017

@author: Michael
"""


from pytrends.request import TrendReq
import pandas as pd

anchor_text = ['ADVANCED MEDICAL TECHNOLOGY ASSOCIATION',
 'AMERICAN SOCIETY OF ANESTHESIOLOGISTS',
 'JDRF',
 'HARVARD UNIVERSITY',
 'HARVARD',
 'HEART',
 'AMAZON',
 'GOOGLE']


for i in range(len(anchor_text) - 1):
    pt1 = TrendReq('google_account_username', 'google_account_password', hl='en-US', tz=360, custom_useragent=None)
    pt1.build_payload([anchor_text[i],anchor_text[i+1]], cat=0, timeframe='2010-01-01 2016-12-31', geo='', gprop='')
    df = pt1.interest_over_time()
    print df
    filepath = 'anchor_trends/anchor_trends' + str(i) + '.csv'
    df.to_csv(filepath)


    