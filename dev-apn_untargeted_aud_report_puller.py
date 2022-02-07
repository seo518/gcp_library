from appnexus import connect
from appnexus import Report
from google.cloud import storage
import pandas as pd
from pandas import DataFrame as df
from io import StringIO
import json
import urllib3
import datetime
import os
# Description: The CF function must have service account role as storage admin
# Inputs: #
# project = string = name of the project that the CF is stored in
# Output: #
# client = client instance object for cloud functions
def initiate_client(project):
    client = storage.Client(project = project)
    return(client)

# Description: Used in cloud functions toupload files from /tmp directory
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
        
# Description: Used in Cloud functions, method to download a file from cloud storage blob into tmp memory
# Inputs: #
# file_key = string = name of the file in the cloud storage, (example: ' GRS_CA_DBM_TT_2019-2019-11-05.csv')
# bucket_name = string = bucket name of where the file_key is located. (example: dmabucketxax)
# Client = object = client oputput from initiate_client() function
# Output: #
# Logical output = returns true if successful download, false if unsuccessful
def download_fromStorage(file_key, bucket_name, client, string):
    file_key = str(file_key)
    tmp_key = file_key.split("/")
    tmp_key = tmp_key[len(tmp_key)-1]
    print(tmp_key)
    print(file_key)
    try:
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(file_key)
        if string is True:
            s = blob.download_as_string()
            return(s)
        else:
            blob.download_to_filename("/tmp/"+tmp_key)
            print(True)
            print("/tmp/"+tmp_key)
            return("/tmp/"+tmp_key)
    except:
        print(False)
            
def exec_source(source_file, bucket_name = 'xax-functions'):
    client = initiate_client(project = 'mp-adh-groupm-ca')
    s = download_fromStorage(file_key = source_file, bucket_name = bucket_name, client = client, string = True)
    s = s.decode('utf-8')
    exec(s, globals())
    print('Source executed')

def passPrint(event, context):
    print(1)

def request_get(url, headers = None):
    http = urllib3.PoolManager()
    if headers is None:
        r = http.request('GET', url)
    else:
        r = http.request('GET', url, headers = headers)
    return(r)

def APN_API_URL(api_url):
    x = request_get(url = api_url)
    x = json.loads(x.data.decode('utf-8'))
    return(x)

def log_APN(user, pwd):
    x = connect(user, pwd)
    return (x)

def main():

    ## Load in all the initial configurations ##
    client = initiate_client(project = 'mp-adh-groupm-ca')
    config = json.loads(download_fromStorage(file_key = "apn/untargeted_report.json", 
                                         bucket_name = "xax-configs", client = client, string = True))
    api_urls = json.loads(download_fromStorage(file_key = "apn/auth_url.json", 
                                           bucket_name = "xax-configs", client = client, string = True))
    api_login = json.loads(download_fromStorage(file_key = "apn/apnauth.json", 
                                           bucket_name = "xax-configs", client = client, string = True))
    
    ## Main ## 
    connect(api_login['auth']['username'], api_login['auth']['password'])
    report = Report(config["untargeted report"]).save()
    data = report.download()
    f = StringIO(str(data, 'utf-8'))
    f_name = '/tmp/'+datetime.datetime.today().strftime('%Y-%m-%d')+'-apn_untargeted_audience_report'+'.csv'

    f = open(f_name, 'wb')
    f.write(data)    
    upload_file_from_filename(bucket = 'dcm-report-drops', filename = f_name, client = client)

main()