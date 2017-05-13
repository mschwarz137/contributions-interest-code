# -*- coding: utf-8 -*-
"""
Created on Fri Apr 21 17:57:10 2017

@author: Michael
"""


indices = range(16)

import os

import pandas as pd

import sqlite3
conn = sqlite3.connect('npo_data.db')

c = conn.cursor()

#load anchor data, then determine ratios (1 for Google, smaller for others)

#anchor_scale stores scaling ratios - 1 for Google, smaller for others
anchor_scale = {}

anchor_text = ['ADVANCED MEDICAL TECHNOLOGY ASSOCIATION',
 'AMERICAN SOCIETY OF ANESTHESIOLOGISTS',
 'JDRF',
 'HARVARD UNIVERSITY',
 'HARVARD',
 'HEART',
 'AMAZON',
 'GOOGLE']

r = [0.0 for i in range(7)]

for i in range(7):
    #load file into csv
    df_anchor = pd.read_csv('anchor_trends/anchor_trends' + str(i) + '.csv')
    m1 = max(df_anchor[anchor_text[i]])
    m2 = max(df_anchor[anchor_text[i+1]])    
    
    r[i] = float(m1)/float(m2)
    
print r
    
anchor_scale['GOOGLE'] = 1.0
r_so_far = 1.0
for i in range(7):
    r_so_far *= r[6-i]
    anchor_scale[anchor_text[6-i]] = r_so_far

print anchor_scale

df_trends_all = pd.DataFrame(columns = ['ein','trends11','trends12','trends13','trends14'])
df_core_irs_data = pd.DataFrame(columns = ['ein','ntee_code','raw_ntee_code','subseccd','name','sub_name','state','city','zipcode'])

for ind in indices:
    #core_data - irs
    filename_irs = 'irs_name_segments/read_irs_database_succeeded' + str(ind) + '.txt'
    with open(filename_irs, 'r') as infile:
        for line in infile:
            newline1 = line.split(',')
            lennew = len(newline1)
            if lennew > 9:
                newline1 = newline1[0:4] + [' '.join(newline1[4:5+lennew-9])] + newline1[5+lennew-9:]
            new_data = pd.DataFrame(data = [newline1],columns = ['ein','ntee_code','raw_ntee_code','subseccd','name','sub_name','state','city','zipcode'])            
            df_core_irs_data = df_core_irs_data.append(new_data)            
    
    #df_trends_all:
    for filename in os.listdir('nonprofit_trends/' + str(ind)):
        if filename.endswith(".csv"):
            fullfilename = os.path.join('nonprofit_trends/' + str(ind), filename)
                                        
            #import csv file called fullfilename into dataframe
            df_trends = pd.read_csv(fullfilename)
            
            list1 = df_trends.ix[:,1].tolist()
            list2 = df_trends.ix[:,2].tolist()
            
            maxratios = 100.0/float(max(list1))
            
            npo_column = map(lambda x:float(x)*anchor_scale[df_trends.columns.values[1]]*maxratios,list2)
    
            #Here you will compute the total search volume for each of the years 
            
            #12 rows for 2011 (excel row number): 14 - 25
            #for list indices: 12 - 23
            npo_2011_sum = sum(npo_column[12:23])
    
            #12 rows for 2012 (excel row number): 26 - 37
            #for list indices: 24 - 35
            npo_2012_sum = sum(npo_column[24:35])
    
            #12 rows for 2013 (excel row number) 38 - 49
            #for list indices: 36 - 47
            npo_2013_sum = sum(npo_column[36:47])
    
            #12 rows for 2014 (excel row number) 50 - 61
            #for list indices: 48 - 59
            npo_2014_sum = sum(npo_column[48:59])
    
            new_data = pd.DataFrame(data = [[filename[0:9],npo_2011_sum,npo_2012_sum,npo_2013_sum,npo_2014_sum]],columns = ['ein','trends11','trends12','trends13','trends14'])            
            df_trends_all = df_trends_all.append(new_data)
    
                                        
        else:
            pass

#at this point, df_trends_all includes all annual search volume sums and eins for npo's in folders for selected indices



#construct database of answers to yes/no questions

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

#generate list of column names for df_990_12
df_990_12_columns = df_990_12.columns.values.tolist()

#generate list of boolean column names for df_990_12
df_990_12_bool_columns = filter(lambda column_name:df_990_12.ix[0][column_name] in ['Y','N'],df_990_12_columns)

#generate database of boolean columns from df_990_12
df_990_12_bool = df_990_12

sql_string = ''
var_string = ''

for i in range(12):
    ein_column = 'EIN'
    if i == 0:
        df_name1 = df_990_12
        df_string1 = 'df_990_12_bool'
        df_string2 = '12'
    elif i == 1:
        df_name1 = df_990_12_ez
        df_string1 = 'df_990_12_ez_bool'
        df_string2 = '12_ez'
        ein_column = 'ein'
    elif i == 2:
        df_name1 = df_990_12_pf
        df_string1 = 'df_990_12_pf_bool'
        df_string2 = '12_pf'
    elif i == 3:
        df_name1 = df_990_13
        df_string1 = 'df_990_13_bool'
        df_string2 = '13'
    elif i == 4:
        df_name1 = df_990_13_ez
        df_string1 = 'df_990_13_ez_bool'
        df_string2 = '13_ez'
    elif i == 5:
        df_name1 = df_990_13_pf
        df_string1 = 'df_990_13_pf_bool'
        df_string2 = '13_pf'
    elif i == 6:
        df_name1 = df_990_14
        df_string1 = 'df_990_14_bool'
        df_string2 = '14'
    elif i == 7:
        df_name1 = df_990_14_ez
        df_string1 = 'df_990_14_ez_bool'
        df_string2 = '14_ez'
    elif i == 8:
        df_name1 = df_990_14_pf
        df_string1 = 'df_990_14_pf_bool'
        df_string2 = '14_pf'
    elif i == 9:
        df_name1 = df_990_15
        df_string1 = 'df_990_15_bool'
        df_string2 = '15'
    elif i == 10:
        df_name1 = df_990_15_ez
        df_string1 = 'df_990_15_ez_bool'
        df_string2 = '15_ez'
    elif i == 11:
        df_name1 = df_990_15_pf
        df_string1 = 'df_990_15_pf_bool'
        df_string2 = '15_pf'

    #generate list of column names for dataframe df_name1
    df_name1_columns = df_name1.columns.values.tolist()
    
    #generate list of boolean column names for df_name1
    df_name1_bool_columns = filter(lambda column_name:df_name1.ix[0][column_name] in ['Y','N'],df_name1_columns)
    sql_string += ',' + ','.join(map(lambda column_name: df_string1 + '.' + column_name + ' as ' + column_name + df_string2,df_name1_bool_columns))
    var_string += ',' + ','.join(map(lambda column_name: column_name + df_string2,df_name1_bool_columns))
    
    #generate database of boolean columns from df_990_12
    df_name1_bool = df_name1[[ein_column] + df_name1_bool_columns]

    if i == 0:
        df_990_12_bool = df_name1_bool.copy(deep = True)
    elif i == 1:
        df_990_12_ez_bool = df_name1_bool.copy(deep = True)
    elif i == 2:
        df_990_12_pf_bool = df_name1_bool.copy(deep = True)
    elif i == 3:
        df_990_13_bool = df_name1_bool.copy(deep = True)
    elif i == 4:
        df_990_13_ez_bool = df_name1_bool.copy(deep = True)
    elif i == 5:
        df_990_13_pf_bool = df_name1_bool.copy(deep = True)
    elif i == 6:
        df_990_14_bool = df_name1_bool.copy(deep = True)
    elif i == 7:
        df_990_14_ez_bool = df_name1_bool.copy(deep = True)
    elif i == 8:
        df_990_14_pf_bool = df_name1_bool.copy(deep = True)
    elif i == 9:
        df_990_15_bool = df_name1_bool.copy(deep = True)
    elif i == 10:
        df_990_15_ez_bool = df_name1_bool.copy(deep = True)
    elif i == 11:
        df_990_15_pf_bool = df_name1_bool.copy(deep = True)

c.execute('''DROP TABLE IF EXISTS df_990_12_bool''')
df_990_12_bool.to_sql('''df_990_12_bool''',conn)
c.execute('''DROP TABLE IF EXISTS df_990_12_ez_bool''')
df_990_12_ez_bool.to_sql('''df_990_12_ez_bool''',conn)
c.execute('''DROP TABLE IF EXISTS df_990_12_pf_bool''')
df_990_12_pf_bool.to_sql('''df_990_12_pf_bool''',conn)
c.execute('''DROP TABLE IF EXISTS df_990_13_bool''')
df_990_13_bool.to_sql('''df_990_13_bool''',conn)
c.execute('''DROP TABLE IF EXISTS df_990_13_ez_bool''')
df_990_13_ez_bool.to_sql('''df_990_13_ez_bool''',conn)
c.execute('''DROP TABLE IF EXISTS df_990_13_pf_bool''')
df_990_13_pf_bool.to_sql('''df_990_13_pf_bool''',conn)
c.execute('''DROP TABLE IF EXISTS df_990_14_bool''')
df_990_14_bool.to_sql('''df_990_14_bool''',conn)
c.execute('''DROP TABLE IF EXISTS df_990_14_ez_bool''')
df_990_14_ez_bool.to_sql('''df_990_14_ez_bool''',conn)
c.execute('''DROP TABLE IF EXISTS df_990_14_pf_bool''')
df_990_14_pf_bool.to_sql('''df_990_14_pf_bool''',conn)
c.execute('''DROP TABLE IF EXISTS df_990_15_bool''')
df_990_15_bool.to_sql('''df_990_15_bool''',conn)
c.execute('''DROP TABLE IF EXISTS df_990_15_ez_bool''')
df_990_15_ez_bool.to_sql('''df_990_15_ez_bool''',conn)
c.execute('''DROP TABLE IF EXISTS df_990_15_pf_bool''')
df_990_15_pf_bool.to_sql('''df_990_15_pf_bool''',conn)


#write sql_string to file sql_string.txt
sql_string_outfile = open('sql_string.txt','w')
sql_string_outfile.write(sql_string)
sql_string_outfile.close()

var_string_outfile = open('var_string.txt','w')
var_string_outfile.write(var_string[1:])
var_string_outfile.close()


#upload df_trends_all to table with same name in database
c.execute('''DROP TABLE IF EXISTS df_trends_all''')
df_trends_all.to_sql('''df_trends_all''',conn)
#recall that this database has the following columns:
#'ein','trends11','trends12','trends13','trends14'

c.execute('''DROP TABLE IF EXISTS df_core_irs_data''')
df_core_irs_data.to_sql('''df_core_irs_data''',conn)
#recall that this database has the following columns:
#'ein','ntee_code','raw_ntee_code','subseccd','name','sub_name','state','city','zipcode'

#join the following tables together:
#df_trends_all, df_core_irs_data, df_990_12_sub to df_990_15_sub, df_990_12_ez_sub to df_990_15_ez_sub, df_990_12_pf_sub to df_990_15_pf_sub

#to combine with arbitrary list of names, do '''create...''' + list + '''from...'''
c.execute('''DROP TABLE IF EXISTS irs_join_trends''')
c.execute('''CREATE TABLE irs_join_trends AS
SELECT df_trends_all.ein as ein, trends11, trends12, trends13, trends14,
ntee_code, raw_ntee_code, subseccd, name, sub_name, state, city, zipcode,
df_990_12_sub.totcntrbs_12 as totcontrbs_12,
df_990_13_sub.totcntrbs_13 as totcontrbs_13,
df_990_14_sub.totcntrbs_14 as totcontrbs_14,
df_990_15_sub.totcntrbs_15 as totcontrbs_15,
df_990_12_ez_sub.totcntrbs_12 as totcontrbs_12_ez,
df_990_13_ez_sub.totcntrbs_13 as totcontrbs_13_ez,
df_990_14_ez_sub.totcntrbs_14 as totcontrbs_14_ez,
df_990_15_ez_sub.totcntrbs_15 as totcontrbs_15_ez,
df_990_12_pf_sub.totcntrbs_12 as totcontrbs_12_pf,
df_990_13_pf_sub.totcntrbs_13 as totcontrbs_13_pf,
df_990_14_pf_sub.totcntrbs_14 as totcontrbs_14_pf,
df_990_15_pf_sub.totcntrbs_15 as totcontrbs_15_pf'''
+ sql_string +
''' FROM (df_trends_all 
JOIN df_core_irs_data
ON (df_trends_all.ein = df_core_irs_data.ein)
LEFT JOIN df_990_12_sub
ON (df_trends_all.ein = df_990_12_sub.EIN)
LEFT JOIN df_990_13_sub
ON (df_trends_all.ein = df_990_13_sub.EIN)
LEFT JOIN df_990_14_sub
ON (df_trends_all.ein = df_990_14_sub.EIN)
LEFT JOIN df_990_15_sub
ON (df_trends_all.ein = df_990_15_sub.EIN)
LEFT JOIN df_990_12_ez_sub
ON (df_trends_all.ein = df_990_12_ez_sub.EIN)
LEFT JOIN df_990_13_ez_sub
ON (df_trends_all.ein = df_990_13_ez_sub.EIN)
LEFT JOIN df_990_14_ez_sub
ON (df_trends_all.ein = df_990_14_ez_sub.EIN)
LEFT JOIN df_990_15_ez_sub
ON (df_trends_all.ein = df_990_15_ez_sub.EIN)
LEFT JOIN df_990_12_pf_sub
ON (df_trends_all.ein = df_990_12_pf_sub.EIN)
LEFT JOIN df_990_13_pf_sub
ON (df_trends_all.ein = df_990_13_pf_sub.EIN)
LEFT JOIN df_990_14_pf_sub
ON (df_trends_all.ein = df_990_14_pf_sub.EIN)
LEFT JOIN df_990_15_pf_sub
ON (df_trends_all.ein = df_990_15_pf_sub.EIN)
LEFT JOIN df_990_12_bool
ON (df_trends_all.ein = df_990_12_bool.EIN)
LEFT JOIN df_990_12_ez_bool
ON (df_trends_all.ein = df_990_12_ez_bool.ein)
LEFT JOIN df_990_12_pf_bool
ON (df_trends_all.ein = df_990_12_pf_bool.EIN)
LEFT JOIN df_990_13_bool
ON (df_trends_all.ein = df_990_13_bool.EIN)
LEFT JOIN df_990_13_ez_bool
ON (df_trends_all.ein = df_990_13_ez_bool.EIN)
LEFT JOIN df_990_13_pf_bool
ON (df_trends_all.ein = df_990_13_pf_bool.EIN)
LEFT JOIN df_990_14_bool
ON (df_trends_all.ein = df_990_14_bool.EIN)
LEFT JOIN df_990_14_ez_bool
ON (df_trends_all.ein = df_990_14_ez_bool.EIN)
LEFT JOIN df_990_14_pf_bool
ON (df_trends_all.ein = df_990_14_pf_bool.EIN)
LEFT JOIN df_990_15_bool
ON (df_trends_all.ein = df_990_15_bool.EIN)
LEFT JOIN df_990_15_ez_bool
ON (df_trends_all.ein = df_990_15_ez_bool.EIN)
LEFT JOIN df_990_15_pf_bool
ON (df_trends_all.ein = df_990_15_pf_bool.EIN)) as t1''')


conn.commit()

conn.close()