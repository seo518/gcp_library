from google.cloud import storage
import json
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

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

# action must only either be start or stop
def vm_control(action, project = 'mp-adh-groupm-ca', zone = 'us-central1-a', instance = 'exclusive-compute-instance'):
    credentials = GoogleCredentials.get_application_default()
    service = discovery.build('compute', 'v1', credentials=credentials)
    if action == "start":
        request = service.instances().start(project=project, zone=zone, instance=instance)
        response = request.execute()
    if action == "stop":
        request = service.instances().stop(project=project, zone=zone, instance=instance)
        response = request.execute()

def list_instances(project, zone):
    credentials = GoogleCredentials.get_application_default()    
    compute = discovery.build('compute', 'v1', credentials=credentials)    
    result = compute.instances().list(project=project, zone=zone).execute()
    return result

def delete_blob(bucket_name, blob_name, client):
    """Deletes a blob from the bucket."""
    # bucket_name = "your-bucket-name"
    # blob_name = "your-object-name"
    
    storage_client = client
    
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.delete()
    
def action_control (list_files, list_instances):
    if len(list_files)>=1:
        action=list_files[0].split(".")[0].lower()
        if (action== 'start' and list_instances['items'][i]['status'] == 'RUNNING') | (action== 'stop' and list_instances['items'][i]['status'] == 'TERMINATED'):
            exit()
        else:
            vm_control(action=action)
    else:
        print("outcome 2: no outcome")   
        


def main(event, context):
    print(event)
    print(context)
    
    client = initiate_client(project = "mp-adh-groupm-ca")
    f = list_files("r-compute-trigger", client = client)
    
    for n in range(len(f)):
        delete_blob(bucket_name =  "r-compute-trigger", blob_name = f[n],client = client)
    
    r = list_instances(project = 'mp-adh-groupm-ca', zone = 'us-central1-a')
    for i in range(len(r['items'])):
        if r['items'][i]['name'] == 'exclusive-compute-instance':
            break

#list_files = ["start.txt"] 
#list_files = ["stop.txt"]
    action_control(list_files=list_files,list_instances=r)

