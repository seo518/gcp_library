# -*- coding: utf-8 -*-
"""
Created on Wed Jan 13 15:23:41 2021

@author: Shweta.Anjan
"""

import pandas as pd
import numpy as np
import datetime
import numpy as np
import matplotlib
#matplotlib.use('Qt5Agg')
import matplotlib.pyplot as plt
import scipy.stats as ss
import fitter
import sklearn
import seaborn as sns
import matplotlib.pyplot as plt 
from sklearn.preprocessing import LabelEncoder, OneHotEncoder 
import warnings 
warnings.filterwarnings("ignore") 
from sklearn.model_selection import train_test_split 
from sklearn.svm import SVC 
from sklearn.metrics import confusion_matrix

from sklearn import preprocessing
sns.set(color_codes=True)
label_encoder = LabelEncoder()
scaler = preprocessing.StandardScaler()

kba = "C:/Users/shweta.anjan/Downloads/grs_ford_kba_nameplate_2020.csv"
sales = "C:/Users/shweta.anjan/Downloads/grs_ford_sales_nameplate_2020.csv"
delivery = "C:/Users/shweta.anjan/Downloads/grs_ford_activity_nameplate_PR_ATL2020.csv"


df_kba = pd.read_csv(kba)
df_sales = pd.read_csv(sales)
d1 = pd.DataFrame()
df_kba['Report_Date'] = pd.to_datetime(df_kba['Report_Date'])
df_sales['Acquisition_date'] = pd.to_datetime(df_sales['Acquisition_date'])

df_kba['FSA'] = df_kba['FSA'].str.split(',').str[0]

df_activity = pd.read_csv(delivery)

df_sales = df_sales.loc[(df_sales['Acquisition_date'] > '2020-01-01' ) & (df_sales['Acquisition_date'] <= '2020-12-31')]

##### udate the FSA's for Citys where FSA is unknown #####
##### For data sets with both City and FSA #####
# for i in df_kba['City'].unique():
#     temp = df_kba.loc[(df_kba['City'] == i) & (df_kba['Zip_Code'] != 'Unknown')].groupby('Zip_Code')['Total_Conversions'].sum()
#     if temp.empty:
#         d1 = d1.append(df_kba.loc[(df_kba['City'] == i)])
#     if not temp.empty:
#         df_kba['Zip_Code'] = np.where((df_kba['City'] == i) & (df_kba['Zip_Code'] == 'Unknown'), temp.idxmax(), df_kba['Zip_Code'])

activity = df_kba['Activity'].unique()
df_kba = df_kba.drop(['Activity_Id'], axis =1)
df_kba = df_kba.pivot_table('Conversions', ['Report_Date','FSA','Nameplate'],'Activity').reset_index()  
df_kba = df_kba.fillna(0)
df_sales = df_sales.drop(['vehicle_make'], axis = 1)

df_kba['Province'] = np.where(df_kba['FSA'].str[:1] == "E","NB",
                      np.where(df_kba['FSA'].str[:1] == "A","NL",
                      np.where(df_kba['FSA'].str[:1] == "B","NS",
                      np.where(df_kba['FSA'].str[:1] == "C","PE", 
                      np.where(df_kba['FSA'].str[:1] == "R","MB", 
                      np.where(df_kba['FSA'].str[:1] == "S","SK", 
                      np.where(df_kba['FSA'].str[:1] == "T","AB", 
                      np.where(df_kba['FSA'].str[:1] == "G","QC",
                      np.where(df_kba['FSA'].str[:1] == "H","QC",
                      np.where(df_kba['FSA'].str[:1] == "J","QC",
                      np.where(df_kba['FSA'].str[:1] == "K","ON",
                      np.where(df_kba['FSA'].str[:1] == "L","ON",
                      np.where(df_kba['FSA'].str[:1] == "M","ON",
                      np.where(df_kba['FSA'].str[:1] == "N","ON",
                      np.where(df_kba['FSA'].str[:1] == "P","ON",
                      np.where(df_kba['FSA'].str[:1] == "V","BC",
                      np.where(df_kba['FSA'].str[:1] == "X","NU",
                      np.where(df_kba['FSA'].str[:1] == "Y","YT",
                               None))))))))))))))))))

df_sales['Province'] = np.where(df_sales['FSA'].str[:1] == "E","NB",
                      np.where(df_sales['FSA'].str[:1] == "A","NL",
                      np.where(df_sales['FSA'].str[:1] == "B","NS",
                      np.where(df_sales['FSA'].str[:1] == "C","PE", 
                      np.where(df_sales['FSA'].str[:1] == "R","MB", 
                      np.where(df_sales['FSA'].str[:1] == "S","SK", 
                      np.where(df_sales['FSA'].str[:1] == "T","AB", 
                      np.where(df_sales['FSA'].str[:1] == "G","QC",
                      np.where(df_sales['FSA'].str[:1] == "H","QC",
                      np.where(df_sales['FSA'].str[:1] == "J","QC",
                      np.where(df_sales['FSA'].str[:1] == "K","ON",
                      np.where(df_sales['FSA'].str[:1] == "L","ON",
                      np.where(df_sales['FSA'].str[:1] == "M","ON",
                      np.where(df_sales['FSA'].str[:1] == "N","ON",
                      np.where(df_sales['FSA'].str[:1] == "P","ON",
                      np.where(df_sales['FSA'].str[:1] == "V","BC",
                      np.where(df_sales['FSA'].str[:1] == "X","NU",
                      np.where(df_sales['FSA'].str[:1] == "Y","YT",
                               None))))))))))))))))))



df_activity['Province'] =  np.where(df_activity['FSA'].str[:1] == "E","NB",
                      np.where(df_activity['FSA'].str[:1] == "A","NL",
                      np.where(df_activity['FSA'].str[:1] == "B","NS",
                      np.where(df_activity['FSA'].str[:1] == "C","PE", 
                      np.where(df_activity['FSA'].str[:1] == "R","MB", 
                      np.where(df_activity['FSA'].str[:1] == "S","SK", 
                      np.where(df_activity['FSA'].str[:1] == "T","AB", 
                      np.where(df_activity['FSA'].str[:1] == "G","QC",
                      np.where(df_activity['FSA'].str[:1] == "H","QC",
                      np.where(df_activity['FSA'].str[:1] == "J","QC",
                      np.where(df_activity['FSA'].str[:1] == "K","ON",
                      np.where(df_activity['FSA'].str[:1] == "L","ON",
                      np.where(df_activity['FSA'].str[:1] == "M","ON",
                      np.where(df_activity['FSA'].str[:1] == "N","ON",
                      np.where(df_activity['FSA'].str[:1] == "P","ON",
                      np.where(df_activity['FSA'].str[:1] == "V","BC",
                      np.where(df_activity['FSA'].str[:1] == "X","NU",
                      np.where(df_activity['FSA'].str[:1] == "Y","YT",
                               None))))))))))))))))))
############ Get Data for Prerries and Atlantic ##############

df_kba1 = df_kba[(df_kba['Province'] == 'NB') | (df_kba['Province'] == 'NL') | (df_kba['Province'] == 'NS')|
                 (df_kba['Province'] == 'PE')|(df_kba['Province'] == 'MB')|(df_kba['Province'] == 'SK')|
                 (df_kba['Province'] == 'AB')]
df_kba1['Region'] = np.where(df_kba1['Province'] == "NB","Atl",
                    np.where(df_kba1['Province'] == "NL","Atl",
                    np.where(df_kba1['Province'] == "NS","Atl",
                    np.where(df_kba1['Province'] == "PE","Atl",
                    np.where(df_kba1['Province'] == "MB","Prar",
                    np.where(df_kba1['Province'] == "SK","Prar",
                    np.where(df_kba1['Province'] == "AB","Prar",
                             None)))))))


df_activity['Region'] = np.where(df_activity['Province'] == "NB","Atl",
                    np.where(df_activity['Province'] == "NL","Atl",
                    np.where(df_activity['Province'] == "NS","Atl",
                    np.where(df_activity['Province'] == "PE","Atl",
                    np.where(df_activity['Province'] == "MB","Prar",
                    np.where(df_activity['Province'] == "SK","Prar",
                    np.where(df_activity['Province'] == "AB","Prar",
                             None)))))))


df_sales1 = df_sales[(df_sales['Province'] == 'NB') | (df_sales['Province'] == 'NL') | (df_sales['Province'] == 'NS')|
                 (df_sales['Province'] == 'PE')|(df_sales['Province'] == 'MB')|(df_sales['Province'] == 'SK')|
                 (df_sales['Province'] == 'AB')]

df_sales1['Region'] = np.where(df_sales1['Province'] == "NB","Atl",
                    np.where(df_sales1['Province'] == "NL","Atl",
                    np.where(df_sales1['Province'] == "NS","Atl",
                    np.where(df_sales1['Province'] == "PE","Atl",
                    np.where(df_sales1['Province'] == "MB","Prar",
                    np.where(df_sales1['Province'] == "SK","Prar",
                    np.where(df_sales1['Province'] == "AB","Prar",
                             None)))))))

u  = list(set (df_sales1['FSA'].unique()) - set (df_kba1['FSA'].unique()))
u_df = df_sales1[df_sales1['FSA'].isin(u)]

######### create look back window for sales vs conversions ##############

df_kba1 = df_kba1.groupby(['Report_Date', 'FSA', 'Nameplate','Province', 'Region']).sum().reset_index()
df_sales1 = df_sales1.groupby(['Acquisition_date', 'FSA', 'Name_Plate', 'Province','Region']).sum().reset_index()


df_kba1.to_csv("C:/Users/shweta.anjan/Downloads/df_kba1.csv", index=False)
df_sales1.to_csv("C:/Users/shweta.anjan/Downloads/df_sales1.csv", index=False)


# n = 10
# for conv in activity:
#     df_sales1[conv] = ''
#     for i, j, k in zip(df_sales1['Acquisition_date'], df_sales1['FSA'], df_sales1['Name_Plate']):
#     #temp_date = i - datetime.timedelta(days= 10)
#     #for conv in activity:
#         sum=0
#         for day  in (range(1, n+1)):
#             temp = i - datetime.timedelta(days = day)
#             temp_conv = df_kba1[(df_kba1['Report_Date'] == temp) & (df_kba1['FSA']== j) & (df_kba1['Nameplate'] == k)][conv]
#             if not temp_conv.empty:
#                 sum = sum+temp_conv.values[0]
# # !             #df_sales1[(df_sales1['Acquisition_date'] == temp) & (df_sales1['FSA']== j) & 
#                 #(df_sales1['Name_Plate'] == k)][conv] = sum   
#         df_sales1[conv] = np.where((df_sales1['Acquisition_date'] == i) & (df_sales1['FSA']== j) & (df_sales1['Name_Plate'] == k), sum ,df_sales1[conv])       
                

# for conv in activity:
#     df_sales1[conv] = ''


# n = 10
# for i, j, k in zip(df_sales1['Acquisition_date'], df_sales1['FSA'], df_sales1['Name_Plate']):
#     print(i)
#     print(j+' '+k)
#     #temp_date = i - datetime.timedelta(days= 10)
#     for conv in activity:
#         print(conv)
#         sum=0
#         for day  in (range(1, n+1)):
#             temp = i - datetime.timedelta(days = day)
#             temp_conv = df_kba1[(df_kba1['Report_Date'] == temp) & (df_kba1['FSA']== j) & (df_kba1['Nameplate'] == k)][conv]
#             if not temp_conv.empty:
#                 sum = sum+temp_conv.values[0]
#             print('sum=' + str(sum))    
#         df_sales1[conv] = np.where((df_sales1['Acquisition_date'] == i) & (df_sales1['FSA']== j) & (df_sales1['Name_Plate'] == k), sum ,df_sales1[conv])


                
# n = 10
# for i, j, k in zip(df_sales1['Acquisition_date'], df_sales1['FSA'], df_sales1['Name_Plate']):
#     #temp_date = i - datetime.timedelta(days= 10)
#     for conv in activity:
#         sum=0
#         for day  in (range(1, n+1)):
#             temp = i - datetime.timedelta(days = day)
#             temp_conv = df_kba1[(df_kba1['Report_Date'] == temp) & (df_kba1['FSA']== j) & (df_kba1['Nameplate'] == k)][conv]
#             if not temp_conv.empty:
#                 sum = sum+temp_conv.values[0]
#         df_sales1.at[list(set(df_sales1.index[df_sales1['Acquisition_date'] == temp].tolist()).intersection(df_sales1.index[df_sales1['FSA'] == j].tolist()).intersection(df_sales1.index[df_sales1['Name_Plate'] == k].tolist()))[0],conv] = sum
                       
                
                
                


########### By Sales By FSA By Model #############

df = pd.read_csv("C:/Users/shweta.anjan/Downloads/grs_sales_kba_lookback_finalresults.csv")
df = df.drop(['unixDate', 'join'], axis=1)

###### total relation between media and sales #######

df_total = df.groupby(['FSA', 'Name_Plate', 'Province', 'Region']).sum().reset_index()
fit1 = df_total.drop(['FSA','Name_Plate','Province','Region'], axis=1).columns
df_total = df_total.fillna(0).replace(np.inf,0)
#df_total.loc[:,fit1]= scaler.fit_transform(df_total.loc[:,fit1])
df_corr= df_total.corr(method='spearman')


mask = np.triu(np.ones_like(df_corr, dtype=np.bool))
fig, ax = plt.subplots(figsize=(10, 8))
# mask
mask = np.triu(np.ones_like(df_corr, dtype=np.bool))
# adjust mask and df
mask = mask[1:, :-1]
corr = df_corr.iloc[1:,:-1].copy()
# plot heatmap
sns.heatmap(corr, mask=mask, annot=True, fmt=".2f", cmap='Reds',
           vmin=-1, vmax=1, cbar_kws={"shrink": .8})
# yticks
plt.yticks(rotation=0)
plt.xticks(rotation=45)
plt.show()


for model in df['Name_Plate'].unique():
    temp =  df.loc[(df['Name_Plate'] == model)]

    fit1 = temp.drop(['Acquisition_date','FSA','Name_Plate','Province','Region','unixDate', 'join'], axis=1).columns
    temp = temp.fillna(0).replace(np.inf,0)
    temp.loc[:,fit1]= scaler.fit_transform(temp.loc[:,fit1])
    
    temp.hist()
    plt.subplots_adjust(hspace=0.5) 
    plt.legend(labels= temp[fit1].columns)
    for j in temp[fit1].columns:
        print(j)
        sns.distplot(temp[j], hist=False, rug=True)  
     
    temp_corr= temp.corr(method='spearman')
    sns.set(font_scale=1.25)
    y = sns.heatmap(temp_corr, cmap ='OrRd', fmt="d")
    y.set_xticklabels(fit1 ,rotation=25, weight = 'bold')
    y.set_yticklabels(fit1, weight = 'bold')
    y.xaxis.set_ticks_position('top')
    
    n=len(temp_corr.columns)
    t=temp_corr*np.sqrt((n-2)/(1-temp_corr*temp_corr))
    temp_pvalue = ss.t.cdf(t, n-2)
    temp_pvalue = pd.DataFrame(temp_pvalue, columns = temp_corr.columns, index=temp_corr.columns)
    
    df_fsa_corr = pd.DataFrame(columns = temp.columns)
    df_fsa_corr['FSA'] = temp['FSA'].unique()
    df_fsa_p = pd.DataFrame(columns = temp.columns)
    df_fsa_p['FSA'] = temp['FSA'].unique()
    
    for i in temp['FSA'].unique():  
        fsa = temp[temp['FSA'].str.contains(i)]
        temp_corr_fsa = fsa.corr(method='spearman')
        # t=temp_corr_fsa*np.sqrt((n-2)/(1-temp_corr_fsa*temp_corr))
        # pvalue_fsa = ss.t.cdf(t, n-2)
        # pvalue_fsa = pd.DataFrame(pvalue_fsa, columns=temp_corr_fsa.columns, index=temp_corr_fsa.columns)
        for j in temp.columns:
            if temp_corr['SaleNumber'][j] > 0.25:
                if temp_pvalue['SaleNumber'][j] > 0.1 :
                    df_fsa_corr.loc[df_fsa_corr.FSA == i,j]= temp_corr['SaleNumber'][j]
                    df_fsa_p.loc[df_fsa_p.FSA == i,j] = temp_pvalue['SaleNumber'][j]

