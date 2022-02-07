from google.cloud import storage

# Description: The CF function must have service account role as storage admin
# Inputs: #
# project = string = name of the project that the CF is stored in
# Output: #
# client = client instance object for cloud functions
def initiate_client(project):
    client = storage.Client(project = project)
    return(client)

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

def main():
    client = initiate_client(project = 'mp-adh-groupm-ca')
    test = {
      "test": [
          {
            "test": "test"
          }
      ]
    }
    with open("/tmp/test.json", 'w') as f:
        json.dump(test, f, indent=4, sort_keys=True)
    upload_file_from_filename(bucket = 'r-data-output', filename = "/tmp/test.json", client = client)

main()