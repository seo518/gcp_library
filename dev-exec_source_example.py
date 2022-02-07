from google.cloud import storage

# Description: The CF function must have service account role as storage admin
# Inputs: #
# project = string = name of the project that the CF is stored in
# Output: #
# client = client instance object for cloud functions
def initiate_client(project):
    client = storage.Client(project = project)
    return(client)

# Description: Used in Cloud functions, method to download a file from cloud storage blob into tmp memory
# Inputs: #
# file_key = string = name of the file in the cloud storage, (example: ' GRS_CA_DBM_TT_2019-2019-11-05.csv')
# bucket_name = string = bucket name of where the file_key is located. (example: dmabucketxax)
# Client = object = client oputput from initiate_client() function
# Output: #
# Logical output = returns true if successful download, false if unsuccessful
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
    
def exec_source(source_file, bucket_name = 'xax-functions'):
    client = initiate_client(project = 'mp-adh-groupm-ca')
    s = download_fromStorage(file_key = source_file, bucket_name = bucket_name, client = client, string = True)
    s = s.decode('utf-8')
    exec(s, globals())
    print('Source executed')

def passPrint(event, context):
    print(1)
    
    

