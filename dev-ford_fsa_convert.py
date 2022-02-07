import googlemaps
from datetime import datetime
from google.cloud import storage
import os
import json
import time
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
    
    
def main():
    client = initiate_client(project = 'mp-adh-groupm-ca')
    
    api_key = download_fromStorage(file_key = "google/maps_api_key.txt", bucket_name = "xax-configs", client = client, string = True)
    api_key = api_key.decode("utf-8")
    
    inputs_key = list_files("xax-input-mapsapi", client = client)[0]
                 
    input_text = download_fromStorage(file_key = zipcode, bucket_name = "xax-input-mapsapi", client = client, string = True)
    input_list = pd.read_csv(input_text)
    input_list = input_list['zipcode']
    # input_text = input_text.decode("utf-8")
    # input_list = input_text.split("\r\n")
    
    delete_blob(bucket_name =  "xax-input-mapsapi", blob_name = inputs_key,client = client)
    
    for n in range(len(input_list)):
        # Geocoding an address
        gmaps = googlemaps.Client(key=api_key)    
        geocode_result = gmaps.geocode(input_list.iloc[n])
        print(geocode_result)
        
        if len(geocode_result) == 0:
            gmaps = googlemaps.Client(key=api_key)
            geocode_result = gmaps.places(input_list.iloc[n])
            print(geocode_result)
            if geocode_result['status'] is 'ZERO_RESULTS':
                gmaps = googlemaps.Client(key=api_key)
                geocode_result = gmaps.places("M4W")
                print(geocode_result)
                gmaps = googlemaps.Client(key=api_key)
                geocode_result = gmaps.places(input_list.iloc[n])
                print(geocode_result)
        geocode_result = geocode_result[0]
        try:
            outputname = dir_name+input_list[n]+fname+'.json'
            with open(outputname, 'w') as outfile:
                json.dump(geocode_result, outfile)    
            upload_file_from_filename(bucket = "xax-output-mapsapi", filename = outputname, client = client)
            time.sleep(2)
        except:
            outputname = dir_name+input_list[n]+fname+'.json'
            with open(outputname, 'w') as outfile:
                json.dump('NaN', outfile)   
            upload_file_from_filename(bucket = "xax-output-mapsapi", filename = outputname, client = client)
            time.sleep(1)            
        print(n)

main()