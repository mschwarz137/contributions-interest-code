# -*- coding: utf-8 -*-
"""
Created on Sat Apr 22 03:28:27 2017

@author: Michael
"""
from bokeh.models import  Callback, ColumnDataSource, Rect, Select,CustomJS
from bokeh.plotting import figure, output_file, show,  gridplot
from bokeh.models.widgets.layouts import VBox,HBox
import numpy as np
from bokeh.models import Legend

import bokeh as bk

import pandas as pd

import sqlite3

from bokeh.models import HoverTool
from bokeh.io import output_notebook

import struct as st
from scipy import stats


from bokeh.layouts import row
from bokeh.palettes import Viridis3
from bokeh.models import CheckboxGroup

from bokeh.models import Range1d, LabelSet, Label


plot_to_render = 1;

#gives total contributions for by looking through entries for three different types of forms (990,990ez,990pf)
def select_total_contributions(row,year_str):
    #yearstr is 12, 13, 14, or 15
    if (type(row['totcontrbs_' + year_str]) == str or type(row['totcontrbs_' + year_str]) == long or type(row['totcontrbs_' + year_str]) == int or type(row['totcontrbs_' + year_str]) == float):
        return row['totcontrbs_' + year_str]
    if (type(row['totcontrbs_' + year_str + '_ez']) == str or type(row['totcontrbs_' + year_str + '_ez']) == long or type(row['totcontrbs_' + year_str + '_ez']) == int or type(row['totcontrbs_' + year_str + '_ez']) == float):
        return row['totcontrbs_' + year_str + '_ez']
    if (type(row['totcontrbs_' + year_str + '_pf']) == str or type(row['totcontrbs_' + year_str + '_pf']) == long or type(row['totcontrbs_' + year_str + '_pf']) == int or type(row['totcontrbs_' + year_str + '_pf']) == float):
        return row['totcontrbs_' + year_str + '_pf']

def ntee_to_simp(ntee):#returns first letter of ntee, or 'none' if ntee is None
    if(ntee == None):
        return 'none'
    if(type(ntee) == str):
        return ntee[0]
    if(type(ntee) == unicode):
        return str(ntee)[0]
    return 'none'#catch all

conn = sqlite3.connect('npo_data.db')
c = conn.cursor()

#load table irs_join_trends, which has the following columns:
#ein, trends11, trends12, trends13, trends14, 
#ntee_code, raw_ntee_code, subseccd, name, subname, state, city, zipcode,
#totcontrbs_12 through totcntrbs_15
#totcontrbs_12_ez through totcntrbs_15_ez
#totcontrbs_12_pf through totcntrbs_15_pf

#load table into pandas dataframe

irs_join_trends = pd.read_sql('''select * from irs_join_trends''',conn)

#create column totcontrbs_12_use in pandas dataframe, (likewise for 13, 14, 15)
#if type of totcontrbs_12 entry is int, float, or str, set to that. Else: repeat for other two (ez,pf)
#same for 13,14,15

for year_str in ['12','13','14','15']:
    irs_join_trends['totcontrbs_' + year_str + '_use'] = irs_join_trends.apply(lambda row: select_total_contributions(row,year_str),axis = 1)

irs_join_trends['totcontrbs_sum'] = irs_join_trends['totcontrbs_12_use'] + irs_join_trends['totcontrbs_13_use'] + irs_join_trends['totcontrbs_14_use'] + irs_join_trends['totcontrbs_15_use']
irs_join_trends['trends_sum'] = irs_join_trends['trends11'] + irs_join_trends['trends12'] + irs_join_trends['trends13'] + irs_join_trends['trends14']


#three plots:
    
#1. total contributions (4 years) vs. Google search volume
if (plot_to_render == 1):
    gsv = irs_join_trends['trends_sum'].tolist()
    tc = irs_join_trends['totcontrbs_sum'].tolist()
    on = irs_join_trends['name'].tolist()
    ntees = irs_join_trends['ntee_code'].tolist()
    
    gton = zip(gsv,tc,on,ntees)
    
    #filter out gsv 0s:
    for i in range(len(gsv)):
        ind1 = len(gsv) - 1 - i
        if gsv[ind1] == 0.0:
            del gton[ind1]
            
    #list of subplot keys to use in dicts of subplot data
    subplotkeys = [chr(code1) for code1 in range(ord('A'),ord('Z')+1)] + ['none']
    gton_subplots = {key1:filter(lambda tup:(ntee_to_simp(tup[3]) == key1),gton) for key1 in subplotkeys}
    gsv_sub = {key1:[ent[0] for ent in gton_subplots[key1]] for key1 in subplotkeys}
    tc_sub = {key1:[ent[1] for ent in gton_subplots[key1]] for key1 in subplotkeys}
    on_sub = {key1:[ent[2] for ent in gton_subplots[key1]] for key1 in subplotkeys}
    
    
    output_file("contributions_vs_search_volume_4_years5-12-17.html")
    output_notebook()
    
    #make source a dict:
        
    source = [ColumnDataSource(data = dict(x = gsv_sub[key1],y = tc_sub[key1], desc = on_sub[key1])) for key1 in subplotkeys]
    
    hover = HoverTool(
            tooltips=[
                ("Organization Name", "@desc"),
            ]
        )
    
    p = figure(plot_width=1200, plot_height=800, tools=[hover],
               title="Total Contributions vs. Google Search Volume for the Years 2011-2014 for Selected Nonprofits (All NTEE Classifications)",x_axis_type = "log",y_axis_type ="log",x_axis_label = "Google Search Volume",y_axis_label = "Total Contributions (Dollars)")
    
    colred = [69,170,137,113,66,219,147,208,186,169]
    colgreen = [115,70,165,88,152,132,169,147,205,155]
    colblue = [167,68,78,143,175,61,208,146,150,190]
    
    color1 = zip(colred,colgreen,colblue)
    labels1 = ['A. Arts, Culture, and Humanities',
    'B. Education',
    'C. Environment',
    'D. Animal-Related',
    'E. Health Care',
    'F. Mental Health and Crisis Intervention',
    'G. Diseases, Disorders, and Medical Disciplines',
    'H. Medical Research',
    'I. Crime and Legal-Related',
    'J. Employment',
    'K. Food, Agriculture, and Nutrition',
    'L. Housing and Shelter',
    'M. Public Safety, Disaster Preparedness and Relief',
    'N. Recreation and Sports',
    'O. Youth Development',
    'P. Human Services',
    'Q. International, Foreign Affairs, and National Security',
    'R. Civil Rights, Social Action, and Advocacy',
    'S. Community Improvement and Capacity Building',
    'T. Philanthropy, Voluntarism, and Grantmaking Foundations',
    'U. Science and Technology',
    'V. Social Science',
    'W. Public and Societal Benefit',
    'X. Religion-Related',
    'Y. Mutual and Membership Benefit',
    'Z. Unknown',
    'None']
    
    pc = [p.circle('x', 'y', size=5, source=source[indp],color = color1[indp]) for indp in range(0,10)]+[p.triangle('x', 'y', size=5, source=source[indp],color = color1[indp-10]) for indp in range(10,20)]+[p.square('x', 'y', size=5, source=source[indp],color = color1[indp-20]) for indp in range(20,27)]
    
    legend = Legend(items=zip(labels1,map(lambda x:[x],pc)), location=(0, -100))
    
    checkbox = CheckboxGroup(labels=labels1,active=range(27), width=100)
    
    checkbox.callback = CustomJS.from_coffeescript(args=dict(pc0=pc[0],pc1=pc[1],pc2=pc[2],pc3=pc[3],pc4=pc[4],pc5=pc[5],pc6=pc[6],pc7=pc[7],pc8=pc[8],pc9=pc[9],pc10=pc[10],pc11=pc[11],pc12=pc[12],pc13=pc[13],pc14=pc[14],pc15=pc[15],pc16=pc[16],pc17=pc[17],pc18=pc[18],pc19=pc[19],pc20=pc[20],pc21=pc[21],pc22=pc[22],pc23=pc[23],pc24=pc[24],pc25=pc[25],pc26=pc[26], checkbox=checkbox), 
    code=''.join(['pc' + str(indp) + '.visible = ' + str(indp) + ' in checkbox.active;' for indp in range(27)]))
    
    
    p.add_layout(legend, 'right')
    layout = row(checkbox, p)
    
    
    show(layout)
    
if (plot_to_render == 2):
    gsv = irs_join_trends['trends_sum'].tolist()
    gsv11 = irs_join_trends['trends11'].tolist()
    gsv12 = irs_join_trends['trends12'].tolist()
    gsv13 = irs_join_trends['trends13'].tolist()
    gsv14 = irs_join_trends['trends14'].tolist()
    tc = irs_join_trends['totcontrbs_sum'].tolist()
    tc11 = irs_join_trends['totcontrbs_12_use'].tolist()
    tc12 = irs_join_trends['totcontrbs_13_use'].tolist()
    tc13 = irs_join_trends['totcontrbs_14_use'].tolist()
    tc14 = irs_join_trends['totcontrbs_15_use'].tolist()
    on = irs_join_trends['name'].tolist()
    ntees = irs_join_trends['ntee_code'].tolist()
    
    gton = zip(gsv,gsv11,gsv12,gsv13,gsv14,tc,tc11,tc12,tc13,tc14,on,ntees)
    
    #filter out gsv 0s:
    for i in range(len(gsv)):
        ind1 = len(gsv) - 1 - i
        if gsv[ind1] == 0.0:
            del gton[ind1]
            
    [gsv,gsv11,gsv12,gsv13,gsv14,tc,tc11,tc12,tc13,tc14,on,ntees] = zip(*gton)
    
    on_sorted = sorted(list(set(on)))
    gsv_single = {on[i]:[gsv11[i],gsv12[i],gsv13[i],gsv14[i]] for i in range(len(gsv))}
    tc_single = {on[i]:[tc11[i],tc12[i],tc13[i],tc14[i]] for i in range(len(gsv))}
    
    

    
    output_file('contributions_vs_search_volume_single_organization.html')
    
    #Figure for Stacked bar chart
    p1 = figure(title="contributions vs. interest for single organization, 2011-2014",x_axis_label = "Google Search Volume",y_axis_label = "Total Contributions (Dollars)", 
                plot_width=800, plot_height = 800)
    
    
    #source for callback
    source1 = ColumnDataSource(data=dict(x=gsv_single['AMERICAN HEART ASSOCIATION INC'], y = tc_single['AMERICAN HEART ASSOCIATION INC']))
    source2 = ColumnDataSource(data = gsv_single)
    source3 = ColumnDataSource(data = tc_single)
    
    p1.circle('x', 'y', size=5, source=source1)
    p1.text('x','y',source=source1,text=['2011','2012','2013','2014'],text_font_size='10pt',text_color='black')

    Callback_A = CustomJS(args={'source1': source1, 'gsvsingle' : source2, 'tcsingle' : source3}, code="""
            var f = cb_obj.get('value');
            var data1 = source1.get('data');
            var data2 = gsvsingle.get('data');
            var data3 = tcsingle.get('data');
            data1['x'] = data2[f];
            data1['y'] = data3[f];
            source1.trigger('change');
        """)
    #Use the Select widget
    dropdown_age = Select(title="Organization:", value='AMERICAN HEART ASSOCIATION INC', options= on_sorted,  callback = Callback_A)
    
    #Display data
    filters = VBox(dropdown_age)
    tot =  HBox(filters, gridplot([[p1]]))
    show(tot)    
    
    
conn.commit()

conn.close()