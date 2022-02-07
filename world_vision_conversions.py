# -*- coding: utf-8 -*-
"""
Created on Thu Jul  9 16:00:45 2020

@author: Shweta.Anjan
"""

# -*- coding: utf-8 -*-

from google.cloud import storage
import pandas as pd
import os
import csv
import numpy as np

# Testing function. Passes the function.
def passPrint():
    print(1)

# Testing function. Creates a test file in /tmp director with subdirectory of /dir
def createTestFile():
    basedir = "/tmp/dir"
    if not os.path.exists(basedir):
        os.makedirs(basedir)
        print(True)


# Description: Because cloud functions do not have datetime, we use an API to get the date as an ordinal date.
# Inputs # 
# None
# Output # 
# Ordinal Date = String = example '2019-309'
def getordinalDate():
    def left(s, amount):
        return s[:amount]    
    import ast
    import json
    import requests
    r = requests.get('http://worldtimeapi.org/api/timezone/America/Toronto')
    r = r.json()
    return(left(r['utc_datetime'],10))

# Description: Converts a list to a string
# Inputs: #
# l = string = any list
# Output: #
# string = string = string output format of the list
def listToString(l):  
    string = ""  
    for e in l:  
        string += e  
    return string

# Description: The CF function must have service account role as storage admin
# Inputs: #
# project = string = name of the project that the CF is stored in
# Output: #
# client = client instance object for cloud functions
def initiate_client(project):
    client = storage.Client(project = project)
    return(client)

# Description: Lists available files in given bucket, including sub directories within the bucket
# Inputs: # 
# project = string = name of the project that the CF is stored in
# bucket = string = bucket name in cloud storage, must already exists
# Client = object = client oputput from initiate_client() function
# Output: #
# listblobs = list = list of strings containing all the filenames in the given bucket 
def list_files(bucket, client):
    bucket = client.get_bucket(bucket)
    blobs = bucket.list_blobs()
    listblobs = []
    for blob in blobs:
        listblobs.append(blob.name)
        print(blob.name)
    return(listblobs)

# Description: Used in Cloud functions, method to download a file from cloud storage blob into tmp memory
# Inputs: #
# file_key = string = name of the file in the cloud storage, (example: ' GRS_CA_DBM_TT_2019-2019-11-05.csv')
# bucket_name = string = bucket name of where the file_key is located. (example: dmabucketxax)
# Client = object = client oputput from initiate_client() function
# Output: #
# Logical output = returns true if successful download, false if unsuccessful
def download_fromStorage(file_key, bucket_name, client):
    file_key = str(file_key)
    try:
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(file_key)
        blob.download_to_filename("/tmp/"+file_key)
        print(True)
    except:
        print(False)

# Description: Used in cloud functions to upload files from /tmp directory
# Inputs: #
# filename = string = full path in tmp file
#   example: '/tmp/DV360_DMA_2019-06-06.csv' 
# Output: #
# Logical print output = true 
# true = file uploaded successfuly
# If no output, then file was uploaded unsuccessfully
# Client = object = client oputput from initiate_client() function
def upload_file_from_filename(bucket, filename, client):
    try:
        bucket = client.get_bucket(bucket)
        blob = bucket.blob(os.path.basename(filename))
        blob.upload_from_filename(filename)
        print(True)
    except:
        print(False)


def read_file(file: str) -> pd.DataFrame:
    """
    Read the file
    :param f: filename
    :return:
    """
    with open(file, "r") as f:
        reader = csv.reader(f)
        for i, line in enumerate(reader):
            if 'Activity Date/Time' in line:
                skiprows = i
                break
    print('number of rows to skip: ', skiprows)
    df = pd.read_csv(file, skiprows=skiprows, parse_dates=['Activity Date/Time'])
    return df

def pre_process_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    preprocess the dataframeï¼Œsplit the string based on the comma
    :param df: The original report from DCM floodlight
    :return: Dataframe after the process
    """
    df['Product Brand (string)'] = df['Product Brand (string)'].str.strip()
    df['Product Price (string)'] = df['Product Price (string)'].str.strip()
    df['Product Brand (string)'] = df['Product Brand (string)'].str.split(',')
    df['Product Price (string)'] = df['Product Price (string)'].str.split(',')
    df = df[df.iloc[:, 0] != 'Grand Total:']  # exclude the last row
    return df


### this function aggregates Geo's in a DCM files into DMAs for US and CANADA using a DMA refrence file ####
### @param : dcm_data : Performce  report from DCM for US and Canada
### @param : tt_target_dma : The DMa targeted for TT campaigns, file in GCS link URL "https://storage.cloud.google.com/grs_reference_files/tt_target_DMA.csv"
###                             This target DMa file changes based on advertiser or campaign requirement
### @param : CA_ref_dma : master reference file for CA DMA's. file in GCS link URL "https://storage.cloud.google.com/grs_reference_files/master_ca_ref_dma.csv"
###                       this file remains same for most of the GEO based campaings 
### Excluded : the list of cities that need to be excluded from the segregation
def wv_conversions(file):
    df1 = read_file(file)
    
    if len(df1) >= 3:
        df1 = pre_process_df(df1)
        count= 0
        for i,j,k in zip(df1['Product Price (string)'],df1['Product Brand (string)'],df1['Total Revenue']):
            
            if "un" in '/t'.join(i):
                print(i)
                df1['Product Price (string)'][count] = [str(k)]
                                                            
                if k == 39:
                    print(k)
                    df1['Product Brand (string)'][count] = ["Child"]
                else:    
                    df1['Product Brand (string)'][count] = ["One Time Donation"]
            if count < df1.shape[0]:
                count = count+1       
        
        df1['Product Price (string)'] = df1['Product Price (string)'].apply(lambda x: list(map(float, x)))
         
        for i,j,k,l in zip(df1['Product Price (string)'],df1['Total Revenue'], df1['Total Conversions'],df1['Product Brand (string)']):
            if sum(i) != j:
                if len(i) != k:
                    dif_conv = k-len(i)
                    print(dif_conv)
                    dif_rev = j-sum(i)
                    factor = (j-sum(i))/(k-len(i))
                    print(factor)
                    q=0
                    for n in range(0, dif_conv):
                        print(i)
                        i.append(factor)
                        if factor % 39 == 0 :
                            l.append("Pledge")
                        else:                 
                            l.append("One Time Donation")
                        print(l)    
        count = 0         
        for i,j,k in zip(df1['Total Conversions'], df1['View-through Conversions'], df1['Click-through Conversions']) :
            if i > 1:
                df1['Total Conversions'][count] = 1
            if j > 1:
                df1['View-through Conversions'][count] = 1
            if k > 1:
                df1['Click-through Conversions'][count] = 1
            count = count+1     
        df1_split= df1.explode('Product Price (string)').drop('Product Brand (string)',axis=1)
        df1_split['Product Brand (string)'] = df1.explode('Product Brand (string)')['Product Brand (string)']
        
        df1_split['Product Brand (string)'][df1_split['Product Price (string)'] == 39] = "Child"
        
        df1_split['Single Revenue'] = df1_split['Product Price (string)']
        
        df1_split['Revenue'] = df1_split['Product Price (string)']      
        df1_split['Revenue'][df1_split['Product Brand (string)'].str.contains("Child")] = df1_split['Product Price (string)'] * 36
        df1_split['Revenue'][df1_split['Product Brand (string)'].str.contains("Ple")] = df1_split['Product Price (string)'] * 30
        
        df1_split['Language'] = np.where(df1_split.Placement.str.contains('_EN_'), 'EN', 'FR')
        
        df1_split['Date'] =  pd.to_datetime(df1_split['Date'])
        
        df1_split['Date'] = df1_split['Date'].dt.date
        df1_split = df1_split[df1_split['Product ID (string)'] != "undefined"]
       #The return file should be dumped into storage   
        return(df1_split)
    else:
        return(False)
    
    
def main(): 

    # initiate storage client
    client = initiate_client(project = 'mp-adh-groupm-ca')
    
    print("Filter for main file needed for the dbm_dma_ca function for today's dates")
    list_blobs = list_files(bucket = 'dcm-report-drops', client = client)
    today = getordinalDate()
    print('Ordinal Date:')
    print(today)
    list_f = list(filter(lambda x:'World_Vision_FloodLight' in x, list_blobs))
    list_f = list(filter(lambda x:today in x, list_f))

    
    print('Load in necessary files into memory')
    d = download_fromStorage(file_key=listToString(list_f), bucket_name='dcm-report-drops', client = client)
    print(d)
     
    print('Execute main function')
    df = wv_conversions(file = str('/tmp/'+listToString(list_f)))
    
    if df is False:
        print("No content in file")
    else:
        print('Save to file and upload')
        file_name = '/tmp/'+today+'-wv_conversions_2020.csv'
        df.to_csv(file_name, encoding ='utf-8', index = False)
        upload_file_from_filename(bucket = 'xax-cloudfunctions-outputs', filename = file_name, client = client)

main()