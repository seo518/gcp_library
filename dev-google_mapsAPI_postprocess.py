import pandas as pd
import json
import ast
import os
from google.cloud import storage

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
            blob.download_to_filename("/tmp/"+tmp_key)
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

def main():
    client = initiate_client(project = 'mp-adh-groupm-ca')
    maps_api_files = list_files(bucket = 'xax-output-mapsapi', client = client)
    for m in range(len(maps_api_files)):
        x = json.loads(download_fromStorage(file_key = maps_api_files[0] , bucket_name = 'xax-output-mapsapi', client = client, string = True))
        x = ast.literal_eval(x)
        
        a = list(x.keys())
        p_code = []
        for i in range(len(a)):
            print(type(x[a[i]]))
            if type(x[a[i]])==dict:
                print(1)
                p_code.append("")
                next
            else:
                print(2)
                for n in range(len(x[a[i]][0]['address_components'])):
                    print(3)
                    print(x[a[i]][0]['address_components'][n]['types'][0]=='postal_code')
                    if x[a[i]][0]['address_components'][n]['types'][0]=='postal_code':
                        p_code.append(x[a[i]][0]['address_components'][n]['short_name']+"--"+a[i])
                        break
        df = pd.DataFrame(data = p_code, columns = ["p_code"])
        df[['postal_code','latlong']] = df.p_code.str.split("--", expand = True)
        fname = "/tmp/"+maps_api_files[m]+".csv"
        df.to_csv(fname, index = False)
        upload_file_from_filename(bucket = 'xax-output-mapsapi-postprocess', filename = fname, client = client)

main()
