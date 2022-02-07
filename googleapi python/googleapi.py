from __future__ import print_function
import json
from google_auth_oauthlib import flow
from googleapiclient.discovery import build
import json
from datetime import datetime
import urllib3
import re

## Initial static variables
working_dir = "D:/Documents/bitbucket/gcp_library/googleapi python/APIJson"
config_file = working_dir+"/apiAccessCreds.json"
api_urls = working_dir+"/API_URL_GOOG.json"
google_endpoint = 'https://accounts.google.com/o/oauth2/token'
token_list_file = working_dir+"/tokens.json"

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
#def gcp_uploadtoStorage(bucket_name, file_name, project_name, credentials = compute_engine.Credentials()):
    #client = storage.Client(credentials=credentials, project=project_name)
    #bucket = client.get_bucket(bucket_name)
    #blob = bucket.blob(os.path.basename(file_name))
    #blob.upload_from_filename(file_name)
    
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

## READ JSON FILE 
def read_json(file_path):
    with open(file_path) as json_file:
        r = json.load(json_file)
    return(r)

## Non GCF function
def create_token(client_config, scope_url):
    appflow = flow.InstalledAppFlow.from_client_config(client_config,
                                                             scopes=[scope_url])
    appflow.run_local_server()
    credentials = appflow.credentials
    return(credentials)

def import_config(config_file):
    x = read_json(config_file)
    return(x)

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

## PATCH REQUEST
def request_patch(url, data, headers = None):
    if type(data) is str:
        http = urllib3.PoolManager()
        if headers is None:
            r = http.request('PATCH', url,
                             headers={'Content-Type': 'application/json'},
                             body=data)
        else:
            r = http.request('{', url,
                             headers=headers,
                             body=data)
    else:
        raise print("Data in patch request must be string")
    return(r)

## client_config = appflow key in the config list
## scope_url = scope URL of the service
## current_token = the token list that has all the tokens
## new_token_name = name of your new token
## token_list_file = the file where your token list is located
def generate_token(client_config, scope_url, current_token, new_token_name, token_list_file):
    config = import_config(config_file = config_file)
    creds = create_token(client_config = client_config, scope_url = scope_url)
    
    token = {
        "access_token": creds.token,
        "client_id": creds.client_id,
        "client_secret":creds.client_secret,
        "expiry":creds.expiry.strftime("%Y-%m-%d %H-%M-%S"),
        "refresh_token":creds.refresh_token,
        "scopes":creds.scopes
    }
    current_token[new_token_name] = token
    with open(token_list_file, 'w') as f:
        json.dump(current_token, f, indent=4, sort_keys=True)    
    return(current_token)

def refresh_token(endpoint, token):
    d = {
        "client_id" : token['client_id'],
        "client_secret" : token['client_secret'],
        "grant_type" : "refresh_token",
        "refresh_token" : token['refresh_token']
    }
    print(json.dumps(d))
    print(endpoint)
    req = request_post(url = endpoint, data = json.dumps(d))
    req = json.loads(req.data.decode("utf-8"))
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

## DV360 METHODS
def advertiser_lineItems_list(advertiser_id, api_url, token):
    api_url = re.sub("{ADVERTISER_ID}",str(advertiser_id), api_url)
    x = request_get(url = api_url, headers = {"Authorization": "Bearer "+ token})
    x = json.loads(x.data)
    return(x)

## Patch data must be a string json
## Example patch_data:
##{
##    "budget": NEW BUDGET OBJECT,
##    "pacing": NEW PACING OBJECT
##}
def advertiser_lineItems_patch(patch_data, advertiser_id, lineItem_id, api_url, token):
    api_url = re.sub("{ADVERTISER_ID}",str(advertiser_id), api_url)
    api_url = re.sub("{LINE_ITEM_ID}",str(lineItem_id), api_url)
    x = request_patch(url = api_url,data = patch_data, headers = {"Authorization": "Bearer "+ token})
    x = json.loads(x.data)
    return(x)    

def targetingOptionsGeoSearch(advertiser_id,search_key, api_url, token):
    payload = {
      "advertiserId": advertiser_id,
      "geoRegionSearchTerms": {
        "geoRegionQuery": search_key
      }
    }
    x = request_post(url = api_url, data = json.dumps(payload), headers = {"Authorization": "Bearer "+ token})
    x = json.loads(x.data)
    return(x)

def createTargetingOption(advertiser_id, lineItem_id, targeting_type, targetingOptionId, api_url, token, negative = False):
    api_url = re.sub("{ADVERTISER_ID}",str(advertiser_id), api_url)
    api_url = re.sub("{LINE_ITEM_ID}",str(lineItem_id), api_url)
    api_url = re.sub("{TARGETING_TYPE}",str(targeting_type), api_url)
    payload = {
        "targetingOptionId": targetingOptionId,
        "negative": negative
    }
    x = request_post(url = api_url, data = json.dumps(payload), headers = {"Authorization": "Bearer "+ token})
    x = json.loads(x.data)
    return(x)

def bulkEditTargetingOption(advertiser_id, lineItem_id, request, targeting_type, targetingOptionId, token, request_type = "create"):
    api_url = re.sub("{ADVERTISER_ID}",str(advertiser_id), api_url)
    api_url = re.sub("{LINE_ITEM_ID}",str(lineItem_id), api_url)
    if request_type is "create":
        payload = {
          "createRequests": [
              {
                "targetingType": targeting_type,
                "assignedTargetingOptionIds": [
                  targetingOptionId
                ]
              }              
          ]
        }
    else:
        payload = {
          "deleteRequests": [
              {
                "targetingType": targeting_type,
                "assignedTargetingOptionIds": [
                  targetingOptionId
                ]
              }              
          ]
        }
    x = request_post(url = api_url, data = json.dumps(payload), headers = {"Authorization": "Bearer "+ token})
    x = json.loads(x.data)
    return(x)    

## SAMPLE
#advertiser_id = 3082331
#token_list = read_json(working_dir+"/tokens.json")
#config = import_config(config_file = config_file)
#creds = create_token(client_config = config['appflow'], scope_url = config['Creds']['Google']['Scopes']['dfatracking'])

#token_list = generate_token(client_config = config['appflow'], scope_url = config['Creds']['Google']['Scopes']['dv360'], current_token = token_list, new_token_name = 'miguel_dfa', token_list_file = working_dir+"/tokens.json")

#refreshed_token = refresh_token(endpoint = google_endpoint, token = token_list['miguel_dv360'])


#refreshed_token

#def get_dbm_LI(advertiser_id, token):
    #url = 'https://displayvideo.googleapis.com/v1/advertisers/' + advertiser_id + '/lineItems'
    #x = request_get(url = url,  headers={"Authorization": "Bearer " + token})
    #return (x)

#x = get_dbm_LI(advertiser_id="1309251", token = refreshed_token['access_token'])

#api_urls = read_json(api_urls)

#lineItems = advertiser_lineItems_list(advertiser_id = "3082331", api_url = api_urls['dv360']['advertiser_lineItems_list'], token = refreshed_token['access_token'])


#a = targetingOptionsGeoSearch(advertiser_id = 3082331, search_key = "M4W", api_url = api_urls['dv360']['advertiser_lineItems_geoSearchTargetingOption'], token = refreshed_token['access_token'])

#oauth2.0_refresh <- function(endpoint, app, auth_token, type = NULL) {
  #req <- POST(
    #url = endpoint$access,
    #multipart = FALSE,
    #body = list(
      #client_id = app$key,
      #client_secret = app$secret,
      #grant_type = "refresh_token",
      #refresh_token = auth_token$refresh_token
    #)
  #)
  #content_out <- content(req, type = type)
  #content_out <- c(content_out, auth_token$refresh_token)
#}

