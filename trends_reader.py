

# -*- coding: utf-8 -*-
"""
Created on Thu Apr 20 02:18:44 2017

@author: Michael
"""


from pytrends.request import TrendReq
import pandas as pd
import numpy as np
import re
import time

#indices of data segments to read (goes from 0 to about 300)
indices1 = [15]
recover_failed = False

anchor_text = ['ADVANCED MEDICAL TECHNOLOGY ASSOCIATION',
 'AMERICAN SOCIETY OF ANESTHESIOLOGISTS',
 'JDRF',
 'HARVARD UNIVERSITY',
 'HARVARD',
 'HEART',
 'AMAZON',
 'GOOGLE']

default_anchor = 1
anchor_sample_length = 10
               
def name_cleaner(name):
    return re.sub(r'[\s]USA','',re.sub(r'[\s]LLC','',re.sub(r'[\s]INC$','',re.sub(r'[\s]INCORPORATED', '', name))))

time.sleep(1)

pt1 = TrendReq('google_account_username', 'google_account_password', hl='en-US', tz=360, custom_useragent=None)

total_tries = 0;
    
for i in indices1:
    #open irs_name_segments/read_irs_database_succeeded + str(i) + .txt
    #read eins and names from file, while cleaning up names, then close
    eins = []
    names = []
    eins_names = []
    anchor_queue = [];
                   
    #open failed list and succeeded list for write
    
    if recover_failed:
        with open('nonprofit_trends/trends_failed' + str(i) + '.txt', 'r') as file1:
            for line in file1:
                irs_data = line.split(',')
                ein = irs_data[0]
                eins.append(ein)
                name = name_cleaner(irs_data[1])
                names.append(name)         
        ff = open('nonprofit_trends/trends_failed' + str(i) + '.txt','w')
        fs = open('nonprofit_trends/trends_succeeded' + str(i) + '.txt','a')

    else:                   
        with open('irs_name_segments/read_irs_database_succeeded' + str(i) + '.txt', 'r') as file1:
            for line in file1:
                irs_data = line.split(',')
                ein = irs_data[0]
                eins.append(ein)
                name = name_cleaner(irs_data[4])
                names.append(name)
        ff = open('nonprofit_trends/trends_failed' + str(i) + '.txt','w')
        fs = open('nonprofit_trends/trends_succeeded' + str(i) + '.txt','w')                                                            
    
    eins_names = zip(eins,names)

    for e_n in eins_names:
        
        time.sleep(11)
        
        ein = e_n[0];
        name = e_n[1];
                  
        print ein
        print name
                  
        try:
                                                             
            #set anchor to default anchor, and make pytrends call    
            anchor = default_anchor
            
            tries = 0
            tried = set([])
            while True:

                
                tries += 1
                total_tries += 1

                pt1.build_payload([anchor_text[anchor],name], cat=0, timeframe='2010-01-01 2016-12-31', geo='', gprop='')
                df1 = pt1.interest_over_time()
                m1 = max(df1[anchor_text[anchor]])
                m2 = max(df1[name])
                print 'm1:', m1
                print 'm2:', m2
                
                if tries >len(anchor_text):
                    break
                
                if anchor in tried:
                    print 'part 5'
                    break
                tried.add(anchor)
                
                #if m2<30, then if anchor not 0, make anchor 1 smaller, otherwise break
                if (m2 < 30):
                    if (anchor==0):
                        print 'part 1'
                        break
                    else:
                        print 'part 2'
                        anchor -= 1
                if (m1 < 10):
                    if (anchor==len(anchor_text)-1):
                        print 'part 3'
                        break
                    else:
                        print 'part 4'
                        anchor += 1
                        

                        
            print tries
            
            filepath = 'nonprofit_trends/' + str(i) + '/' + ein + '.csv'
            df1.to_csv(filepath)
            
            #add to queue
            anchor_queue.append(anchor)
            
            #if queue is long enough, remove entry from queue and make median into default anchor
            if len(anchor_queue)>anchor_sample_length:
                anchor_queue=anchor_queue[1:]
                default_anchor = int(np.median(anchor_queue))

            #save ein,name to succeeded_list
            fs.write(ein + ',' + name + '\n')            
        except:
            #save ein,name to failed_list
            ff.write(ein + ',' + name + '\n')
    #close failed list and succeeded list
    fs.close()
    ff.close()

#this may be helpful for rate limit information    
print 'total_tries:', total_tries
