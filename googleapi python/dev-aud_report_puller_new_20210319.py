import json
import urllib3
import os
import ast
from google.cloud import storage
import datetime



# what does it mean?
def passPrint(event, context):
    print(google_auth("dfa"))
    print(1)


def initiate_client(project):
    client = storage.Client(project=project)
    return (client)


# Description: Lists available files in given bucket, including sub directories within the bucket
# listblobs = list = list of strings containing all the filenames in the given bucket
def list_files(bucket, client):
    bucket = client.get_bucket(bucket)
    blobs = bucket.list_blobs()
    listblobs = []
    for blob in blobs:
        listblobs.append(blob.name)
        print(blob.name)
    return (listblobs)


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
    tmp_key = tmp_key[len(tmp_key) - 1]
    print(tmp_key)
    print(file_key)
    try:
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(file_key)
        if string is True:
            s = blob.download_as_string()
            return (s)
        else:
            blob.download_to_filename("/tmp/" + tmp_key)
            print(True)
            print("/tmp/" + tmp_key)
            return ("/tmp/" + tmp_key)
    except:
        print(False)


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


# request google to get token
def request_post(url, data, headers=None):
    if type(data) is str:
        http = urllib3.PoolManager()
        if headers is None:
            r = http.request('GET', url,
                             headers={'Content-Type': 'application/json'},
                             body=data,
                             verify=False)
        else:
            r = http.request('GET', url,
                             headers=headers,
                             body=data,
                             verify=False)
    else:
        raise print("Data in post request must be string")

    return (r)


# refresh access code
def oauth2_refresh(endpoint, app, auth_token, type=None):
    d = {
        "client_id": app['client_id'][0],
        "client_secret": app['client_secret'][0],
        "grant_type": "refresh_token",
        "refresh_token": auth_token['refresh_token'][0]
    }
    print(json.dumps(d))
    print(endpoint)
    req = request_post(url=endpoint, data=json.dumps(d))
    return (req)


def read_json(file_path):
    with open(file_path) as json_file:
        r = json.load(json_file)
    return (r)


# Description: use pre-authenticated token to get a refreshed token. Callable only from router
# Inputs:
#   platform = string = bidmanager OR dfa or a google product
# Outputs:
#   token = full response token from google authentication scope
def google_auth(platform):
    # Get configs
    app = "google/APIJson/apiAccessCreds.json"
    auth_token = "google/google.tokens.json"
    client = initiate_client(project='mp-adh-groupm-ca')
    app = read_json(download_fromStorage(file_key=app, bucket_name='xax-configs', client=client, string=False))
    auth_token = read_json(
        download_fromStorage(file_key=auth_token, bucket_name='xax-configs', client=client, string=False))
    app = app['Creds']['Google']
    auth_token = auth_token[platform]  # change later

    token = oauth2_refresh(endpoint='https://accounts.google.com/o/oauth2/token', app=app, auth_token=auth_token)
    token = token.data.decode("utf-8")

    # TODO implement
    return {
        'statusCode': 200,
        'body': token
    }


def exec_source(source_file, bucket_name='xax-functions'):
    client = initiate_client(project='mp-adh-groupm-ca')
    s = download_fromStorage(file_key=source_file, bucket_name=bucket_name, client=client, string=True)
    s = s.decode('utf-8')
    exec(s, globals())
    print('Source executed')


def request_get(url, headers=None):
    http = urllib3.PoolManager()
    if headers is None:
        r = http.request('GET', url)
    else:
        r = http.request('GET', url, headers=headers)
    return (r)


def DBM_listQueries(api_url, token):
    x = request_get(url=api_url, headers={"Authorization": "Bearer " + token})
    x = json.loads(x.data.decode('utf-8'))
    return (x)


def DBM_createQueryList(listQueries):
    queryIds = []
    for n in range(len(listQueries['queries'])):
        queryIds.append(listQueries['queries'][n]['queryId'])
    return (queryIds)


def DBM_getQueryLatestReport(queryId, queryList, reportList, token):
    file_url = reportList['queries'][queryList.index(queryId)]['metadata']['googleCloudStoragePathForLatestReport']
    x = request_get(url=file_url, headers={"Authorization": "Bearer " + token})
    return (x)

def main():
    
    ## Load in all the initial configurations ##
    client = initiate_client(project = 'mp-adh-groupm-ca')
    exec_source("dev-google_get_auth_2.py")
    token = google_auth("yelu_dfa")
    config = json.loads(download_fromStorage(file_key = "google/audience_report_puller_config_new.json", bucket_name = "xax-configs", client = client, string = True))
    api_urls = json.loads(download_fromStorage(file_key = "google/API_URL_GOOG.json", bucket_name = "xax-configs", client = client, string = True))
    bidmanager_token = google_auth("yelu_bidmanager")
    
    ## Main ##       
    ## DBM Sequence ##
    dbm_reports = DBM_listQueries(api_url = api_urls['bidmanager']['listQueries'], 
                        token = json.loads(bidmanager_token['body'])['access_token'])
    queryList = DBM_createQueryList(listQueries = dbm_reports)
    for i in range(len(config['report_puller']['dbm_report_ids'])):
        csv_write = DBM_getQueryLatestReport(queryId = config['report_puller']['dbm_report_ids'][i], 
        queryList = queryList, 
        reportList = dbm_reports, 
        token = json.loads(bidmanager_token['body'])['access_token'])
        f_name = '/tmp/'+datetime.datetime.today().strftime('%Y-%m-%d')+'-'+config['report_puller']['dbm_report_names'][i]+'.csv'
        #f_name = 'C:/tmp/'+datetime.datetime.today().strftime('%Y-%m-%d')+'-'+config['report_puller']['dbm_report_names'][i]+'.csv'
        
        f = open(f_name, 'wb')
        f.write(csv_write.data)
        f.close()        
        upload_file_from_filename(bucket = 'dcm-report-drops', filename = f_name, client = client)

main()