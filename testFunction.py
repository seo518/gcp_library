from google.cloud import storage
import datetime 
import os
import json

def initiate_client(project):
    client = storage.Client(project = project)
    return(client)

def testgcs(event, context):
    client = storage.Client(project = 'mp-adh-group-ca')
    bucket = client.get_bucket('dmabucketxax')
    blobs = bucket.list_blobs()
    listblobs = []
    for blob in blobs:
        listblobs.append(blob.name)
        print(blob.name)

def createTestFile():
    basedir = "/tmp/dir"
    if not os.path.exists(basedir):
        os.makedirs(basedir)
        print(True)
        
def upload_file_from_filename(bucket, filename, client = initiate_client(project = 'mp-adh-group-ca')):
    try:
        bucket = client.get_bucket(bucket)
        blob = bucket.blob(os.path.basename(filename))
        blob.upload_from_filename(filename)
        print(True)
    except:
        print(False)

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

def main():
    print(1)
    
main()