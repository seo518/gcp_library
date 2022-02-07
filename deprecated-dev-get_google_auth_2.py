#' author: Miguel Ajon
import json
import urllib3
import os
import ast 
from google.cloud import storage

def passPrint(event, context):
    print(google_auth("dfa"))
    print(1)

# Description: The CF function must have service account role as storage admin
# Inputs: #
# project = string = name of the project that the CF is stored in
# Output: #
# client = client instance object for cloud functions
def initiate_client(project):
    client = storage.Client(project = project)
    return(client)

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
    tmp_key = file_key.split("/")
    tmp_key = tmp_key[len(tmp_key)-1]
    print(tmp_key)
    try:
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(file_key)
        blob.download_to_filename("/tmp/"+tmp_key)
        print(True)
        print("/tmp/"+tmp_key)
        return("/tmp/"+tmp_key)
    except:
        print(False)

def read_json(file_path):
    with open(file_path) as json_file:
        r = json.load(json_file)
    return(r)

# Description: use pre-authenticated token to get a refreshed token. Callable only from router
# Inputs:
#   platform = string = bidmanager OR dfa or a google product
# Outputs:
#   token = full response token from google authentication scope
def google_auth(platform): 
    # Get configs
    app = "google/APIJson/apiAccessCreds.json"
    auth_token = "google/APIJson/google.tokens.json"
    client = initiate_client(project = 'mp-adh-groupm-ca')
    app = read_json(download_fromStorage(file_key=app, bucket_name='xax-configs1', client = client))
    auth_token = read_json(download_fromStorage(file_key=auth_token, bucket_name='xax-configs1', client = client))
    app = app['Creds']['Google']
    auth_token = auth_token[platform] #change later
    
    token = oauth2_refresh(endpoint = 'https://accounts.google.com/o/oauth2/token', app = app, auth_token = auth_token)
    token = token.data.decode("utf-8")
    
    # TODO implement
    return {
        'statusCode': 200,
        'body': token
    }