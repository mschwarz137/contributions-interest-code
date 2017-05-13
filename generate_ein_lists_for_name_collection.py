# -*- coding: utf-8 -*-
"""
Created on Mon Apr 10 01:34:29 2017

@author: Michael
"""

import numpy as np
import pandas as pd
from random import shuffle

import sqlite3
conn = sqlite3.connect('npo_data.db')

overwrite_database = True;

def has_9_digits(n):
    return (len(str(n)) == 9)

c = conn.cursor()

#load appropriate sql tables into pandas tables
df_990_sub_ordered = pd.read_sql('''select * from df_990_sub_ordered''',conn)
df_990_ez_sub_ordered = pd.read_sql('''select * from df_990_ez_sub_ordered''',conn)
df_990_pf_sub_ordered = pd.read_sql('''select * from df_990_pf_sub_ordered''',conn)

#remove from both tables rows without 9 digit eins
df_990_sub_ordered = df_990_sub_ordered[df_990_sub_ordered.apply(lambda x: has_9_digits(x['EIN']), axis=1)]
df_990_ez_sub_ordered = df_990_ez_sub_ordered[df_990_ez_sub_ordered.apply(lambda x: has_9_digits(x['EIN']), axis=1)]
df_990_pf_sub_ordered = df_990_pf_sub_ordered[df_990_pf_sub_ordered.apply(lambda x: has_9_digits(x['EIN']), axis=1)]

#construct a set eins of all the eins in table 1
eins1 = set(df_990_sub_ordered['EIN'].tolist())

#add to form 990 table any rows from form 990 ez and form 990 pf tables corresponding to eins that are not in the set eins
for index, row in df_990_ez_sub_ordered.iterrows():
    if not (row['EIN'] in eins1):
        df_990_sub_ordered = df_990_sub_ordered.append(row)
        
for index, row in df_990_pf_sub_ordered.iterrows():
    if not (row['EIN'] in eins1):
        df_990_sub_ordered = df_990_sub_ordered.append(row)
        
#sort table 1 by totcntrbs_4yr (or appropriate)
df_990_sub_ordered.sort('totcntrbs_4yr',ascending = False, inplace = True)

c.execute('''DROP TABLE IF EXISTS df_990_12_sub_for_name_collection;''')
df_990_sub_ordered.to_sql('''df_990_12_sub_for_name_collection''',conn)

#generate list of eins
list_ein = df_990_sub_ordered['EIN'].tolist()

print 'list_ein:'
print list_ein[:5]

#generate list of totcntrbs_4yr
list_cont = df_990_sub_ordered['totcntrbs_4yr'].tolist()

output_file = open('eins.txt','w')

for ein in list_ein:
    output_file.write(str(ein) + '\n')
    
output_file.close()

conn.commit()

conn.close()