from google.cloud import storage
import os
import urllib3
import json

#### Cloud functions ####

# Testing function. Passes the function.
def passPrint():
    print(1)

##### Cloud Storage Functions for Cloud Functions #####

# Testing function. Creates a test file in /tmp director with subdirectory of /dir
def createTestFile():
    basedir = "/tmp/dir"
    if not os.path.exists(basedir):
        os.makedirs(basedir)
        print(True)

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

# Parsin Functions
def right(s, amount):
    return s[-amount:]
    
def left(s, amount):
    return s[:amount]
    
def mid(s, offset, amount):
    return s[offset:offset+amount]

# Description: Because cloud functions do not have datetime, we use an API to get the date as an ordinal date.
# Inputs # 
# None
# Output # 
# Ordinal Date = String = example '2019-309'
def getordinalDate():
    import ast
    import json
    import requests
    r = requests.get('http://worldtimeapi.org/api/timezone/America/Toronto')
    r = r.json()
    return(left(r['utc_datetime'],10))

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

# Description: Used in Cloud Compute Instance ONLY. Upload function
# Required roles:
# > Storage Admin 
# Required imports:
# > from google.auth import compute_engine
# Inputs: #
# bucket_name = string = name of bucket
# file_name = string = full path file needed to be uploaded
# project name = string = project name
# Credentials = object = authenticated object credentials = default to compute_engine.Credentials()
def gcp_uploadtoStorage(bucket_name, file_name, project_name, credentials = compute_engine.Credentials()):
    client = storage.Client(credentials=credentials, project=project_name)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(os.path.basename(file_name))
    blob.upload_from_filename(file_name)


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

## READ JSON FILE 
def read_json(file_path):
    with open(file_path) as json_file:
        r = json.load(json_file)
    return(r)

## GET REQUEST
def request_get(url, headers = None):
    http = urllib3.PoolManager()
    if headers is None:
        r = http.request('GET', url)
    else:
        r = http.request('GET', url, headers=headers)
    return(r)

## POST REQUEST
def request_post(url, data, headers = None):
    if type(data) is str:
        http = urllib3.PoolManager()
        if headers is None:
            r = http.request('POST', url,
                             headers={'Content-Type': 'application/json'},
                             body=data)
        else:
            r = http.request('POST', url,
                             headers=headers,
                             body=data)
    else:
        raise print("Data in post request must be string")
        
    return(r)

## GET REQUEST
def request_get(url, headers = None):
    http = urllib3.PoolManager()
    if headers is None:
        r = http.request('GET', url)
    else:
        r = http.request('GET', url, headers=headers)
    return(r)

## POST REQUEST
def request_post(url, data, headers = None):
    if type(data) is str:
        http = urllib3.PoolManager()
        if headers is None:
            r = http.request('POST', url,
                             headers={'Content-Type': 'application/json'},
                             body=data)
        else:
            r = http.request('POST', url,
                             headers=headers,
                             body=data)
    else:
        raise print("Data in post request must be string")
        
    return(r)

def oauth2_refresh(endpoint, app, auth_token, type = None):
    d = {
        "client_id" : app['client_id'][0],
        "client_secret" : app['client_secret'][0],
        "grant_type" : "refresh_token",
        "refresh_token" : auth_token['refresh_token'][0]
    }
    print(json.dumps(d))
    print(endpoint)
    req = request_post(url = endpoint, data = json.dumps(d))
    return(req)

## DCM FUNCTIONS
def dcm_getProfileList(api_url, profileId, token):
    x = api_url.replace("{PROFILE_ID}", profileId)
    x = request_get(url = x, headers = {"Authorization": token})
    return(json.loads(x.data.decode("utf-8")))

def dcm_getProfileReportLatest(url_api, profileId, reportId, token):
    url_api = url_api.replace("{PROFILE_ID}", profileId)
    url_api = url_api.replace("{REPORT_ID}", reportId)
    x = request_get(url = url_api, headers = {"Authorization": "Bearer "+ token})
    return(json.loads(x.data.decode("utf-8")))

def dcm_getLatestReport(profileId, reportId, token, url):
    getlatestreport_url = dcm_getProfileReportLatest(url_api = url, 
                                                 profileId = profileId, 
                                                 reportId = reportId, 
                                                 token = token)
    i = 0
    while(1):
        if getlatestreport_url['items'][i]['status'] == 'REPORT_AVAILABLE':
            x = request_get(getlatestreport_url['items'][i]['urls']['apiUrl'], headers = {"Authorization": "Bearer "+ token})
            break
        else:
            i += 1    
    return(x)

## DBM FUNCTIONS
## https://developers.google.com/bid-manager/release-notes
def DBM_listQueries(api_url, token):
    x = request_get(url = api_url, headers = {"Authorization": "Bearer "+ token})
    x = json.loads(x.data.decode('utf-8'))
    return(x)

def DBM_createQueryList(listQueries):
    queryIds = []
    for n in range(len(listQueries['queries'])):
        queryIds.append(listQueries['queries'][n]['queryId'])
    return(queryIds)

def DBM_getQueryLatestReport(queryId, queryList, reportList, token):
    file_url = reportList['queries'][queryList.index(queryId)]['metadata']['googleCloudStoragePathForLatestReport']
    x = request_get(url = file_url, headers = {"Authorization": "Bearer "+ token})
    return(x)


###
#d = "D:/Documents/bitbucket/gcp_library/creds/"
#api_url = read_json(d+"API_URL_GOOG.json")
#token = read_json(d+"google.tokens.json")
#apicreds = read_json(d+"apiAccessCreds.json")
#app = apicreds['Creds']['Google']

#token = oauth2_refresh(endpoint = 'https://accounts.google.com/o/oauth2/token', app = app, auth_token = token)
#token = token.data.decode("utf-8")
#token = json.loads(token)


#dbm_reports = DBM_listQueries(api_url = api_url['bidmanager']['listQueries'][0], token = token['access_token'])
#list_queries = DBM_createQueryList(dbm_reports)
#x = DBM_getQueryLatestReport(queryId="757232590",queryList=list_queries, reportList=dbm_reports, token['access_token'])