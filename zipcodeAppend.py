from datetime import datetime
from google.cloud import storage
import os
import json
import time
import math
import pandas as pd


def initiate_client(project):
    client = storage.Client(project = project)
    return(client)

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
            blob.download_to_filename("C:/tmp/"+tmp_key)
            print(True)
            print("/tmp/"+tmp_key)
            return("/tmp/"+tmp_key)
    except:
        print(False)
        
def upload_file_from_filename(bucket, filename, client):
    try:
        bucket = client.get_bucket(bucket)
        blob = bucket.blob(os.path.basename(filename))
        blob.upload_from_filename(filename)
        print(True)
    except:
        print(False)

def list_files(bucket, client):
    bucket = client.get_bucket(bucket)
    blobs = bucket.list_blobs()
    listblobs = []
    for blob in blobs:
        listblobs.append(blob.name)
        print(blob.name)
    return(listblobs)

def delete_blob(bucket_name, blob_name, client):
    """Deletes a blob from the bucket."""
    # bucket_name = "your-bucket-name"
    # blob_name = "your-object-name"
    
    storage_client = client
    
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.delete()
    
    print("Blob {} deleted.".format(blob_name))

def get_listDif(list_1, list_2):
    return(list(set(list_1) - set(list_2)))

def writelistAsLines(list_x, file):
    f=open(file,'w')
    for ele in list_x:
        f.write(ele+'\n')
    f.close()

def transferBlob(object_x, fromBucket, toBucket, client):
    try:
        object_y = download_fromStorage(file_key = object_x, bucket_name = fromBucket, client = client, string = False)
        upload_file_from_filename(bucket = toBucket, filename = object_y, client = client)
        delete_blob(bucket_name = fromBucket, blob_name = object_x, client = client)
        return(True)
    except:
        return(False)

def main(event = '', context = ''):
    client = initiate_client(project = 'finecast-gcp-ca')
    outputBucket = 'zipcodeappend-output'
    inputBucket = 'zipcodeappend'
    archiveBucket = 'zipcodeappend-archive'
    
    current_outputs = list_files(bucket = outputBucket, client = client)
    current_inputs = list_files(bucket = inputBucket, client = client)
    for n in range(len(current_inputs)):
        appendFrom = current_inputs[n]
        appendTo = list(filter(lambda l: l==appendFrom, current_outputs))
        fname = "/tmp/"+appendFrom
        
        ## read data
        appendFrom_f = download_fromStorage(file_key = appendFrom, bucket_name = outputBucket, client = client, string = False)
        appendFrom_csv = pd.read_csv(appendFrom_f, header = None)
        
        appendTo_f = download_fromStorage(file_key = appendTo[0], bucket_name = inputBucket, client = client, string = False)
        appendTo_csv = pd.read_csv(appendTo_f, header = None)
        
        len(appendFrom_csv)
        len(appendTo_csv)
        appended_csv = pd.concat([appendTo_csv, appendFrom_csv], ignore_index=True, sort=False)
        appended_csv = appended_csv.drop_duplicates()
        appended_csv.to_csv(fname, index = False)
        upload_file_from_filename(filename = fname, bucket = outputBucket, client = client)
        transferBlob(object_x = appendFrom, fromBucket = outputBucket, toBucket = archiveBucket, client = client)