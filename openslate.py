from google.cloud import storage
import json


# Description: The CF function must have service account role as storage admin
# Inputs: #
# project = string = name of the project that the CF is stored in
# Output: #
# client = client instance object for cloud functions
def initiate_client(project):
    client = storage.Client(project=project)
    return (client)


# Description: Lists available files in given bucket, including sub directories within the bucket
# Inputs: #
# project = string = name of the project that the CF is stored in
# bucket = string = bucket name in cloud storage, must already exists
# Client = object = client oputput from initiate_client() function
# Output: #
# listblobs = list = list of strings containing all the filenames in the given bucket
def list_files(bucket, client):
    bucket = client.get_bucket(bucket)
    blobs = bucket.list_blobs()
    listblobs = []
    for blob in blobs:
        listblobs.append(blob.name)
        print(blob.name)
    return (listblobs)


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
    tmp_key = tmp_key[len(tmp_key) - 1]
    try:
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(file_key)
        if string is True:
            s = blob.download_as_string()
            return (s)
        else:
            blob.download_to_filename("/tmp/" + tmp_key)
            print(True)
            print(" download to /tmp/" + tmp_key)
            return ("/tmp/" + tmp_key)
    except:
        print('no file can be downloaded')


# Description: Used in cloud functions to upload files from /tmp directory
# Inputs: #
# filename = string = full path in tmp file
# folder = string = the relative path to the bucket
#   example: '/tmp/DV360_DMA_2019-06-06.csv'
# Output: #
# Logical print output = true
# true = file uploaded successfuly
# If no output, then file was uploaded unsuccessfully
# Client = object = client oputput from initiate_client() function
def upload_file_from_filename(bucket, folder, filename, client):
    try:
        bucket = client.get_bucket(bucket)
        blob = bucket.blob(folder + os.path.basename(filename))
        blob.upload_from_filename(filename)
        print(True)
    except:
        print(False)


def main():
    print('The program starts')
    client = initiate_client(project='mp-adh-groupm-ca')

    # read configuration file
    file = download_fromStorage(file_key="google/openslate-config.json", bucket_name='xax-configs', client=client,
                                string=False)
    f = open(file)
    conf = json.load(f)

    # read configuration
    data_path = "/tmp/"
    dev_function_bucket = conf['openslate']["dev_function_bucket"]
    dev_function_name = conf['openslate']["dev_function_name"]
    input_output_bucket = conf['openslate']["input_output_bucket"]
    input_output_folder = conf['openslate']["input_output_folder"]
    datorama_report_keyword = conf['openslate']['datorama_report_keyword']
    datorama_report_skiprows = conf['openslate']['datorama_report_skiprows']
    dv360_report_keyword = conf['openslate']['dv360_report_keyword']
    dv360_report_skiprows = conf['openslate']['dv360_report_skiprows']
    datorama_key_column = conf['openslate']['datorama_key_column']
    dv360_key_column = conf['openslate']['dv360_key_column']
    datorama_placement = conf['openslate']['datorama_placement']
    dv360_lineitem = conf['openslate']['dv360_lineitem']

    # import external functions
    s = download_fromStorage(file_key=dev_function_name, bucket_name=dev_function_bucket, client=client, string=True)
    s = s.decode('utf-8')
    exec(s, globals())

    # download input file
    list_blobs = list_files(bucket=input_output_bucket, client=client)
    folder = [s for s in list_blobs if input_output_folder in s]    # find the folder path
    final_file = [s for s in folder if 'final.xlsx' in s]    # check whether the program has been trigger before

    if final_file:
        print('output file is already there, exit the program')
    else:
        datorama_filename = [s for s in folder if datorama_report_keyword in s][0]
        print('datorama filename is ', datorama_filename)
        datorama_f = download_fromStorage(file_key=datorama_filename, bucket_name=input_output_bucket,
                                          client=client, string=False)
        dv360_filename = [s for s in folder if dv360_report_keyword in s][0]
        print('dv360_filename is ', dv360_filename)
        dv360_f = download_fromStorage(file_key=dv360_filename, bucket_name=input_output_bucket,
                                       client=client, string=False)

        df_datorama = read_Datorama_file(datorama_f, skiprows=datorama_report_skiprows)
        df_dv360 = read_dv360_file(dv360_f, skiprows=dv360_report_skiprows)
        df_dv360 = process_dv360_report(df_dv360)

        combined, notfound_in_datorama, notfound_in_dv360, \
        openslate_datorama_openslate_dv360, openslate_datorama_no_openslate_dv360, \
        no_openslate_datorama_openslate_dv360, no_openslat_datorama_no_openslate_dv360 = \
            join_datorama_dv360(df_datorama, df_dv360, datorama_key_column, dv360_key_column,
                                datorama_placement, dv360_lineitem)

        # merge the reports with openslate in at least one of the two reports
        final = pd.concat([openslate_datorama_openslate_dv360, openslate_datorama_no_openslate_dv360,
                           no_openslate_datorama_openslate_dv360], axis=0)
        final = aggregate_to_IO(final)

        # output
        f = data_path + 'final.xlsx'
        final.to_excel(f, index=False)
        upload_file_from_filename(bucket=input_output_bucket, folder=input_output_folder, filename=f, client=client)

        f = data_path + 'combined.xlsx'
        combined.to_excel(f, index=False)
        upload_file_from_filename(bucket=input_output_bucket, folder=input_output_folder, filename=f, client=client)

        f = data_path + 'notfound_in_datorama.xlsx'
        notfound_in_datorama.to_excel(f, index=False)
        upload_file_from_filename(bucket=input_output_bucket, folder=input_output_folder, filename=f, client=client)

        f = data_path + 'notfound_in_dv360.xlsx'
        notfound_in_dv360.to_excel(f, index=False)
        upload_file_from_filename(bucket=input_output_bucket, folder=input_output_folder, filename=f, client=client)

        f = data_path + 'openslate_datorama_openslate_dv360.xlsx'
        openslate_datorama_openslate_dv360.to_excel(f, index=False)
        upload_file_from_filename(bucket=input_output_bucket, folder=input_output_folder, filename=f, client=client)

        f = data_path + 'openslate_datorama_no_openslate_dv360.xlsx'
        openslate_datorama_no_openslate_dv360.to_excel(f, index=False)
        upload_file_from_filename(bucket=input_output_bucket, folder=input_output_folder, filename=f, client=client)

        f = data_path + 'no_openslate_datorama_openslate_dv360.xlsx'
        no_openslate_datorama_openslate_dv360.to_excel(f, index=False)
        upload_file_from_filename(bucket=input_output_bucket, folder=input_output_folder, filename=f, client=client)

        f = data_path + 'no_openslat_datorama_no_openslate_dv360.xlsx'
        no_openslat_datorama_no_openslate_dv360.to_excel(f, index=False)
        upload_file_from_filename(bucket=input_output_bucket, folder=input_output_folder, filename=f, client=client)

main()