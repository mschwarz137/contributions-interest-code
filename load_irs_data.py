# -*- coding: utf-8 -*-
"""
Created on Sat Apr 08 02:32:29 2017

@author: Michael
"""

import numpy as np
import pandas as pd
from random import shuffle

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

l_990_12 = list(df_990_12['EIN'])
l_990_12_ez = list(df_990_12_ez['ein'])
l_990_12_pf = list(df_990_12_pf['EIN'])
l_990_13 = list(df_990_13['EIN'])
l_990_13_ez = list(df_990_13_ez['EIN'])
l_990_13_pf = list(df_990_13_pf['EIN'])
l_990_14 = list(df_990_14['EIN'])
l_990_14_ez = list(df_990_14_ez['EIN'])
l_990_14_pf = list(df_990_14_pf['EIN'])
l_990_15 = list(df_990_15['EIN'])
l_990_15_ez = list(df_990_15_ez['EIN'])
l_990_15_pf = list(df_990_15_pf['EIN'])

l = l_990_12 + l_990_12_ez + l_990_13 + l_990_13_ez + l_990_14 + l_990_14_ez + l_990_15 + l_990_15_ez
l_pf = l_990_12_pf + l_990_13_pf + l_990_14_pf + l_990_15_pf

eins = set()
eins_pf = set()
for ein in l:
    if (len(str(ein)) == 9): 
        eins.add(ein)
for ein in l_pf:
    if (len(str(ein)) == 9):
        eins_pf.add(ein)
        
l2 = list(eins)
l2_pf = list(eins_pf)

shuffle(l2)
shuffle(l2_pf)

f = open('ein.dat', 'w')
f_pf = open('ein_pf.dat', 'w')

for ein in l2:
    print>>f, ein
    
for ein in l2_pf:
    print>>f_pf, ein
    
f.close()
f_pf.close()
