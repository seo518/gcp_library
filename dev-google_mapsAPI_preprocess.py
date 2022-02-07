import googlemaps
from datetime import datetime
from google.cloud import storage
import os
import json
import time
import math


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
    
def mapsAPI_getIndex(file_key, client, bucket_name = "xax-configs"):
    maps_api_index = download_fromStorage(file_key = file_key, bucket_name = bucket_name, client = client, string = True)
    if maps_api_index is None:
        print(1)
        maps_api_index = {"index":["maps_init.json"]} # static placeholder, create if not exist
        with open('/tmp/maps_init.json', 'w') as json_file:
            json.dump(maps_api_index, json_file, indent = 4)
        upload_file_from_filename(bucket = bucket_name, filename = "/tmp/maps_init.json", client = client)
        return(maps_api_index)
    if maps_api_index is not None:
        print(2)
        maps_api_index = download_fromStorage(file_key = file_key, bucket_name = bucket_name, client = client, string = True)
        maps_api_index = json.loads(maps_api_index.decode('utf-8'))
        return(maps_api_index)

def mapsAPI_putIndex(maps_api_index, client, bucket_name = "xax-configs"):
    with open('/tmp/maps_init.json', 'w') as json_file:
        json.dump(maps_api_index, json_file, indent = 4)
    upload_file_from_filename(bucket = bucket_name, filename = "/tmp/maps_init.json", client = client)

def mapsAPI_putInputText(file, client, bucket_name = "xax-configs", ):
    upload_file_from_filename(bucket = bucket_name, filename = "/tmp/maps_init.json", client = client)    

def get_listDif(list_1, list_2):
    return(list(set(list_1) - set(list_2)))

def writelistAsLines(list_x, file):
    f=open(file,'w')
    for ele in list_x:
        f.write(ele+'\n')
    f.close()

def main():
    client = initiate_client(project = 'mp-adh-groupm-ca')
    inputs_index = mapsAPI_getIndex(file_key = "maps_init.json", bucket_name = "xax-configs", client = client)
    inputs_list = list_files("xax-input-mapsapi", client = client) 
    inputs_list = get_listDif(list_1 = inputs_list, list_2 = inputs_index['index'])
    for n in range(len(inputs_list)):
        inputs_index['index'].append(inputs_list[n])
    mapsAPI_putIndex(maps_api_index = inputs_index, client = client)
    
    for l in range(len(inputs_list)):
        input_text = download_fromStorage(file_key = inputs_list[l], bucket_name = "xax-input-mapsapi", client = client, string = True)
        input_text = input_text.decode("utf-8").split("\n")

        if len(input_text) >= 50001:
            exit()
            
        time.sleep(5)
        magic_number = 320
        for p in range(math.ceil(len(input_text)/magic_number)):
            print(p)
            fname = "/tmp/"+str(p)+inputs_list[l]
            if p+1 == math.ceil(len(input_text)/magic_number) is True:
                writelistAsLines(list_x = input_text[p*magic_number:], file = fname) ## Change to "/tmp/" later
                upload_file_from_filename(bucket = "xax-inputs-mapsapi-process", filename = fname, client = client)
                print(fname)
            else:
                writelistAsLines(list_x = input_text[p*magic_number:((p+1)*magic_number)], file = fname) ## Change to "/tmp/" later
                upload_file_from_filename(bucket = "xax-inputs-mapsapi-process", filename = fname, client = client)
                print(fname)
            if p == 0:
                time.sleep(10)
            else:
                time.sleep(3)
        delete_blob(bucket_name =  "xax-input-mapsapi", blob_name = inputs_list[n],client = client)

main()
