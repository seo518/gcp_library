import googlemaps
from datetime import datetime
from google.cloud import storage
import os
import json
import time

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
    
def mapsAPI_putInputsResultsConcurrenct(string_x, name, client, bucket_name):
    with open('/tmp/'+name, 'w') as json_file:
        json.dump(string_x, json_file, indent = 4)
    upload_file_from_filename(bucket = bucket_name, filename = "/tmp/"+name, client = client)
    
def main():
    client = initiate_client(project = 'mp-adh-groupm-ca')
    
    inputs_key = list_files("xax-inputs-mapsapi-process", client = client)[0]    
    input_text = download_fromStorage(file_key = inputs_key, bucket_name = "xax-inputs-mapsapi-process", client = client, string = True)
    input_text = input_text.decode("utf-8").split("\n")
    delete_blob(bucket_name =  "xax-inputs-mapsapi-process", blob_name = inputs_key,client = client)
    
    time.sleep(5)
    api_key = download_fromStorage(file_key = "google/maps_api_key.txt", bucket_name = "xax-configs", client = client, string = True)
    api_key = api_key.decode("utf-8")    
    tmp_dict = {}
    for n in range(len(input_text)):
        # Geocoding an address
        if len(input_text[n])==0:
            next
        else:
            gmaps = googlemaps.Client(key=api_key)    
            geocode_result = gmaps.geocode(input_text[n])
            if len(geocode_result) == 0:
                gmaps = googlemaps.Client(key=api_key)
                geocode_result = gmaps.places(input_text[n])
                if geocode_result['status'] is 'ZERO_RESULTS':
                    gmaps = googlemaps.Client(key=api_key)
                    geocode_result = gmaps.places("M4W")
                    gmaps = googlemaps.Client(key=api_key)
                    geocode_result = gmaps.places(input_text[n])
            tmp_dict[input_text[n]] = geocode_result
        time.sleep(0.8)
        print(str(n)+" --- iteration --- "+inputs_key)
        
    mapsAPI_putInputsResultsConcurrenct(string_x = json.dumps(tmp_dict), name = inputs_key+".json", client = client, bucket_name = "xax-output-mapsapi")
    

main()
