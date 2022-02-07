# tester

from google.cloud import storage


# Description: The CF function must have service account role as storage admin
# Inputs: #
# project = string = name of the project that the CF is stored in
# Output: #
# client = client instance object for cloud functions
def initiate_client(project):
    client = storage.Client(project=project)
    return (client)


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
            print("download to /tmp/" + tmp_key)
            return ("/tmp/" + tmp_key)
    except:
        print('no file can be downloaded')


# Description: Used in cloud functions to upload files from /tmp directory
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
    print('The program starts')
    client = initiate_client(project='mp-adh-groupm-ca')

    # import external functions
    s = download_fromStorage(file_key='dev-weather-function.py', bucket_name='xax-functions', client=client,
                             string=True)
    s = s.decode('utf-8')
    exec(s, globals())

    # read configuration file
    file = download_fromStorage(file_key="google/weather-targeting-config.json", bucket_name='xax-configs',
                                client=client,
                                string=False)
    f = open(file)
    conf = json.load(f)

    weather_api_key = conf['weather-targeting']['weather_api_key']
    weather_api_url = conf['weather-targeting']['weather_api_url']
    iTimezone = conf['weather-targeting']['timezone']
    dtime_min = conf['weather-targeting']['start_hour_from_now']
    dtime_max = conf['weather-targeting']['end_hour_from_now']
    number_of_cloud_functions = conf['weather-targeting']['number_of_cloud_functions']

    T_min = (datetime.now(timezone(iTimezone)) + timedelta(hours=dtime_min)).strftime('%d%b%Y_H%H')
    T_max = (datetime.now(timezone(iTimezone)) + timedelta(hours=dtime_max)).strftime('%d%b%Y_H%H')
    output_file = f'{dtime_max - dtime_min}hours_{T_min}-{T_max}-2.csv'

    # download output file
    f = download_fromStorage(file_key=output_file, bucket_name='xax-weather-targeting',
                             client=client, string=False)
    print(f)
    if f:
        print('output file is already there, exit the program')
    else:
        # download apnexus region city to /tmp/ folder, then feed that path to pd.readcsv
        apn_location_file = conf['weather-targeting']['apn_location_file_name']
        f = download_fromStorage(file_key="google/" + apn_location_file, bucket_name='xax-configs',
                                 client=client, string=False)
        df_location = pd.read_csv(f)
        number_rows = df_location.shape[0]
        rows_per_cloudfunction_except_last = int(number_rows / number_of_cloud_functions)
        rows_per_cloudfunction_last = number_rows - rows_per_cloudfunction_except_last * (number_of_cloud_functions - 1)
        skiprows = range(1, rows_per_cloudfunction_except_last + 1) # keep the header
        nrows = rows_per_cloudfunction_except_last
        df_location = pd.read_csv(f, skiprows=skiprows, nrows=nrows)
        df_forecast = weather_forecast(df_location['region_name'], df_location['name'], df_location['id'],
                                       df_location['Latitude'], df_location['Longitude'], weather_api_key,
                                       weather_api_url)
        df_forecast = timewindow(df_forecast, dtime_min, dtime_max)

        # save the file
        print('Save weather forecast file to the bucket')
        print("Tmin_stamp:", T_min, "Tmax_stamp:", T_max, "\n")
        file_name = '/tmp/' + f'{dtime_max - dtime_min}hours_{T_min}-{T_max}-2.csv'
        df_forecast.to_csv(file_name, encoding='utf-8', index=False, sep=',')
        upload_file_from_filename(bucket='xax-weather-targeting', filename=file_name, client=client)

main()
