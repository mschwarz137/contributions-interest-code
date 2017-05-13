# -*- coding: utf-8 -*-
"""
Created on Sat Apr 08 02:32:29 2017

@author: Michael
"""

import numpy as np
import pandas as pd
from random import shuffle

import sqlite3
conn = sqlite3.connect('npo_data.db')

overwrite_database = True;

c = conn.cursor()

df_main = pd.DataFrame(columns = ['ein','totcntrbs_12','totcntrbs_13','totcntrbs_14','totcntrbs_15','totcntrbs_4yr'])

#load 12 files into 12 pandas DataFrames

df_990_12 = pd.read_table('irs_csv/py12_990.dat',sep = ' ')
df_990_12_ez = pd.read_table('irs_csv/py12_990ez.dat',sep = ' ')
df_990_12_pf = pd.read_table('irs_csv/py12_990pf.dat',sep = ' ')
df_990_13 = pd.read_table('irs_csv/py13_990.dat',sep = ' ')
df_990_13_ez = pd.read_table('irs_csv/py13_EZ.dat',sep = ' ')
df_990_13_pf = pd.read_table('irs_csv/py13_990pf.dat',sep = ' ')
df_990_14 = pd.read_table('irs_csv/py14_990.dat',sep = ' ')
df_990_14_ez = pd.read_table('irs_csv/py14_EZ.dat',sep = ' ')
df_990_14_pf = pd.read_table('irs_csv/py14_990pf.dat',sep = ' ')
df_990_15 = pd.read_table('irs_csv/15eofinextract990.dat.dat',sep = ' ')
df_990_15_ez = pd.read_table('irs_csv/15eofinextractEZ.dat',sep = ' ')
df_990_15_pf = pd.read_table('irs_csv/15eofinextract990pf.dat',sep = ' ')

df_990_12_sub = df_990_12.ix[:,['EIN','totcntrbgfts']]
df_990_12_ez_sub = df_990_12_ez.ix[:,['ein','totcntrbs']]
df_990_12_pf_sub = df_990_12_pf.ix[:,['ein','grscontrgifts']]
df_990_13_sub = df_990_13.ix[:,['EIN','totcntrbgfts']]
df_990_13_ez_sub = df_990_13_ez.ix[:,['EIN','totcntrbs']]
df_990_13_pf_sub = df_990_13_pf.ix[:,['ein','grscontrgifts']]
df_990_14_sub = df_990_14.ix[:,['EIN','totcntrbgfts']]
df_990_14_ez_sub = df_990_14_ez.ix[:,['EIN','totcntrbs']]
df_990_14_pf_sub = df_990_14_pf.ix[:,['ein','grscontrgifts']]
df_990_15_sub = df_990_15.ix[:,['EIN','totcntrbgfts']]
df_990_15_ez_sub = df_990_15_ez.ix[:,['EIN','totcntrbs']]
df_990_15_pf_sub = df_990_15_pf.ix[:,['ein','grscontrgifts']]

df_990_12_sub.columns = ['EIN','totcntrbs_12']
df_990_13_sub.columns = ['EIN','totcntrbs_13']
df_990_14_sub.columns = ['EIN','totcntrbs_14']
df_990_15_sub.columns = ['EIN','totcntrbs_15']
df_990_12_ez_sub.columns = ['EIN','totcntrbs_12']
df_990_13_ez_sub.columns = ['EIN','totcntrbs_13']
df_990_14_ez_sub.columns = ['EIN','totcntrbs_14']
df_990_15_ez_sub.columns = ['EIN','totcntrbs_15']
df_990_12_pf_sub.columns = ['EIN','totcntrbs_12']
df_990_13_pf_sub.columns = ['EIN','totcntrbs_13']
df_990_14_pf_sub.columns = ['EIN','totcntrbs_14']
df_990_15_pf_sub.columns = ['EIN','totcntrbs_15']

#place in SQL tables
if overwrite_database:
    c.execute('''DROP TABLE IF EXISTS df_990_12_sub;''')
    c.execute('''DROP TABLE IF EXISTS df_990_13_sub;''')
    c.execute('''DROP TABLE IF EXISTS df_990_14_sub;''')
    c.execute('''DROP TABLE IF EXISTS df_990_15_sub;''')
    c.execute('''DROP TABLE IF EXISTS df_990_12_ez_sub;''')
    c.execute('''DROP TABLE IF EXISTS df_990_13_ez_sub;''')
    c.execute('''DROP TABLE IF EXISTS df_990_14_ez_sub;''')
    c.execute('''DROP TABLE IF EXISTS df_990_15_ez_sub;''')
    c.execute('''DROP TABLE IF EXISTS df_990_12_pf_sub;''')
    c.execute('''DROP TABLE IF EXISTS df_990_13_pf_sub;''')
    c.execute('''DROP TABLE IF EXISTS df_990_14_pf_sub;''')
    c.execute('''DROP TABLE IF EXISTS df_990_15_pf_sub;''')
    df_990_12_sub.to_sql('''df_990_12_sub''',conn)
    df_990_13_sub.to_sql('''df_990_13_sub''',conn)
    df_990_14_sub.to_sql('''df_990_14_sub''',conn)
    df_990_15_sub.to_sql('''df_990_15_sub''',conn)
    df_990_12_ez_sub.to_sql('''df_990_12_ez_sub''',conn)
    df_990_13_ez_sub.to_sql('''df_990_13_ez_sub''',conn)
    df_990_14_ez_sub.to_sql('''df_990_14_ez_sub''',conn)
    df_990_15_ez_sub.to_sql('''df_990_15_ez_sub''',conn)
    df_990_12_pf_sub.to_sql('''df_990_12_pf_sub''',conn)
    df_990_13_pf_sub.to_sql('''df_990_13_pf_sub''',conn)
    df_990_14_pf_sub.to_sql('''df_990_14_pf_sub''',conn)
    df_990_15_pf_sub.to_sql('''df_990_15_pf_sub''',conn)
    
    c.execute('''DROP TABLE IF EXISTS df_990_sub;''')
    c.execute('''CREATE TABLE df_990_sub AS
    SELECT * FROM df_990_12_sub INNER JOIN 
    df_990_13_sub ON df_990_12_sub.EIN=df_990_13_sub.EIN INNER JOIN
    df_990_14_sub ON df_990_12_sub.EIN=df_990_14_sub.EIN INNER JOIN
    df_990_15_sub ON df_990_12_sub.EIN=df_990_15_sub.EIN;''')
    
    c.execute('''ALTER TABLE df_990_sub ADD totcntrbs_4yr INT;''')
    c.execute('''UPDATE df_990_sub SET totcntrbs_4yr = totcntrbs_12+totcntrbs_13+totcntrbs_14+totcntrbs_15;''')
    
    c.execute('''DROP TABLE IF EXISTS df_990_ez_sub;''')
    c.execute('''CREATE TABLE df_990_ez_sub AS
    SELECT * FROM df_990_12_ez_sub INNER JOIN 
    df_990_13_ez_sub ON df_990_12_ez_sub.EIN=df_990_13_ez_sub.EIN INNER JOIN
    df_990_14_ez_sub ON df_990_12_ez_sub.EIN=df_990_14_ez_sub.EIN INNER JOIN
    df_990_15_ez_sub ON df_990_12_ez_sub.EIN=df_990_15_ez_sub.EIN;''')

    c.execute('''ALTER TABLE df_990_ez_sub ADD totcntrbs_4yr INT;''')
    c.execute('''UPDATE df_990_ez_sub SET totcntrbs_4yr = totcntrbs_12+totcntrbs_13+totcntrbs_14+totcntrbs_15;''')

    c.execute('''DROP TABLE IF EXISTS df_990_pf_sub;''')
    c.execute('''CREATE TABLE df_990_pf_sub AS
    SELECT * FROM df_990_12_pf_sub INNER JOIN 
    df_990_13_pf_sub ON df_990_12_pf_sub.EIN=df_990_13_pf_sub.EIN INNER JOIN
    df_990_14_pf_sub ON df_990_12_pf_sub.EIN=df_990_14_pf_sub.EIN INNER JOIN
    df_990_15_pf_sub ON df_990_12_pf_sub.EIN=df_990_15_pf_sub.EIN;''')

    c.execute('''ALTER TABLE df_990_pf_sub ADD totcntrbs_4yr INT;''')
    c.execute('''UPDATE df_990_pf_sub SET totcntrbs_4yr = totcntrbs_12+totcntrbs_13+totcntrbs_14+totcntrbs_15;''')
    
    #order by totcntrbs_4yr
    
    c.execute('''DROP TABLE IF EXISTS df_990_sub_ordered;''')
    c.execute('''CREATE TABLE df_990_sub_ordered AS
    SELECT * FROM df_990_sub ORDER BY totcntrbs_4yr;''')

    c.execute('''DROP TABLE IF EXISTS df_990_ez_sub_ordered;''')
    c.execute('''CREATE TABLE df_990_ez_sub_ordered AS
    SELECT * FROM df_990_ez_sub ORDER BY totcntrbs_4yr;''')
    
    c.execute('''DROP TABLE IF EXISTS df_990_pf_sub_ordered;''')
    c.execute('''CREATE TABLE df_990_pf_sub_ordered AS
    SELECT * FROM df_990_pf_sub ORDER BY totcntrbs_4yr;''')
    

conn.commit()

conn.close()


