import pandas as pd
from google.cloud import storage
import io
import datetime
import os

# Description: The CF function must have service account role as storage admin
# Inputs: #
# project = string = name of the project that the CF is stored in
# Output: #
# client = client instance object for cloud functions
def initiate_client(project):
    client = storage.Client(project=project)
    return (client)

# Description: Used in cloud functions toupload files from /tmp directory
# Inputs: #
# filename = string = full path in tmp file
#   example: '/tmp/DV360_DMA_2019-06-06.csv'
# Output: #
# Logical print output = true
# true = file uploaded successfuly
# If no output, then file was uploaded unsuccessfully
# Client = object = client oputput from initiate_client() function
def upload_file_from_filename(bucket, filename, client):
    try:
        bucket = client.get_bucket(bucket)
        blob = bucket.blob(os.path.basename(filename))
        blob.upload_from_filename(filename)
        print(True)
    except:
        print(False)

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

def split_aud_list(s):
    data = io.StringIO(s)
    df = pd.read_csv(data)
    df = df[df["segment_name"].str.contains("TRB_CA")]
    df[["T1", "T2", "T3", "T4", "T5", "T6", "T7", "T8", "T9", "T10", "T11", "T12", "T13"]] = df[
        "segment_name"].str.split("_", expand=True)
    return(df)

def main():
    client = initiate_client(project='mp-adh-groupm-ca')
    csv_file = download_fromStorage(file_key = datetime.datetime.today().strftime('%Y-%m-%d')+'-apn_untargeted_audience_report'+'.csv',
                                    bucket_name = 'dcm-report-drops',
                                    client = client,
                                    string = True)
    csv_write = split_aud_list(csv_file)
    f_name = '/tmp/'+datetime.datetime.today().strftime('%Y-%m-%d')+'-'+ 'APN_untargeted_aud_report.csv'
    f = open(f_name, 'wb')
    f.write(csv_write.data)
    f.close()
    upload_file_from_filename(bucket = "dcm-report-drops")

main()