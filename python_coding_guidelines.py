# Function Coding Guidelines

# Indents = 4 spaces
# Function parameters must be readable, spaced out
# Function names need to make sense
# All defined functions must contain comments on top of the function 

# Example 1:

# Description: Description of template() function
# Inputs: #
# param1 = string = 'example string'
# param2 = string = defaulted to input1, input 1 = list = ['example', 'list', 'of', 'expected input'] 
# Output: #
# Logical output = True
def template(param1, param2 = input1):
    'code here'
    return(True)


# Example 2:

# Description: Lists available files in given bucket, including sub directories within the bucket
# Inputs: # 
# project = string = name of the project that the CF is stored in
# bucket = string = bucket name in cloud storage, must already exists
# Client = object = client oputput from initiate_client() function
# Output: #
# listblobs = list = list of strings containing all the filenames in the given bucket 
def list_files(project, bucket):
    client = storage.Client(project = project)
    bucket = client.get_bucket(bucket)
    blobs = bucket.list_blobs()
    listblobs = []
    for blob in blobs:
        listblobs.append(blob.name)
    print(blob.name)
    return(listblobs)
