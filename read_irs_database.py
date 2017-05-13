# -*- coding: utf-8 -*-
"""
Created on Thu Apr 06 03:37:39 2017

@author: Michael
"""

import requests
import json

import pandas as pd

from retrying import retry
import time

import sqlite3
conn = sqlite3.connect('npo_data.db')

overwrite_database = True;

c = conn.cursor()

with open('eins.txt') as f:
    einlist = f.readlines()

einlist = [x.strip() for x in einlist]

print len(einlist) 

core_data = pd.DataFrame(columns = ['ein','ntee_code','raw_ntee_code','subseccd','name','sub_name','state','city','zipcode'])

@retry(wait_exponential_multiplier=500,wait_exponential_max=2000,stop_max_attempt_number=3)
def page_data(ein):

    page = requests.get('https://projects.propublica.org/nonprofits/api/v2/organizations/' + ein + '.json')
    data = json.loads(page.text)
    return data

i = 0

ff1 = open('irs_name_segments/read_irs_database_failedA0.txt', 'w')
ff2 = open('irs_name_segments/read_irs_database_failedB0.txt', 'w')
fs = open('irs_name_segments/read_irs_database_succeeded0.txt', 'w')

for ein in einlist:
    
    if ((i%1000 == 0) and (i > 0)):
        ff1.close()
        ff2.close()
        fs.close()
        ir = i/1000
        ff1 = open('irs_name_segments/read_irs_database_failedA' + str(ir) + '.txt', 'w')
        ff2 = open('irs_name_segments/read_irs_database_failedB' + str(ir) + '.txt', 'w')
        fs = open('irs_name_segments/read_irs_database_succeeded' + str(ir) + '.txt', 'w')
    
    
    i += 1
    
    if ((i==10) or (i==100) or (i==1000) or (i%10000 == 0)):
        print i
    
    
    failed = 0
    
    try:
        data = page_data(ein)
    except:
        failed = 1
    
    
    if failed == 0:
        try:
            name = data['organization']['name']
            try:
                sub_name = data['organization']['sub_name']
            except:
                sub_name = 'none'
            try:
                nteecode = data['organization']['ntee_code']
            except:
                nteecode = 'none'
            try:
                rawnteecode = data['organization']['raw_ntee_code']
            except:
                rawnteecode = 'none'
            try:
                subseccd = data['organization']['subseccd']
            except:
                subseccd = '0'    
            try:
                state = data['organization']['state']
            except:
                state = 'none'
            try:
                city = data['organization']['city']
            except:
                city = 'none'
            try:
                zipcode = data['organization']['zipcode']
            except:
                zipcode = 'none'
            new_data = pd.DataFrame(data = [[str(ein),str(nteecode),str(rawnteecode),str(subseccd),str(name),str(sub_name),str(state),str(city),str(zipcode)]],columns = ['ein','nteecode','rawnteecode','subseccd','name','sub_name','state','city','zipcode'])
            core_data = core_data.append(new_data)
            fs.write(','.join(map(lambda x:str(x),[ein, nteecode, rawnteecode, subseccd, name, sub_name,state,city,zipcode]))+'\n')
        except:
            ff1.write(ein + '\n')
            pass
    else:
        ff2.write(ein + '\n')
    

if overwrite_database:
    c.execute('''DROP TABLE IF EXISTS form990''')
    core_data.to_sql('''form990''',conn)

conn.commit()

conn.close()
    
ff1.close()
ff2.close()
fs.close()
