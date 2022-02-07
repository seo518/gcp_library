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
    try:
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(file_key)
        if string is True:
            s = blob.download_as_string()
            return(s)
        else:
            blob.download_to_filename("/tmp/"+tmp_key)
            print(True)
            print(" download to /tmp/"+tmp_key)
            return("/tmp/"+tmp_key)
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
    print('the main program starts! ')
    client = initiate_client(project='mp-adh-groupm-ca')

    # import external functions
    s = download_fromStorage(file_key='dev-weather-function.py', bucket_name='xax-functions', client=client, string=True)
    s = s.decode('utf-8')
    exec(s, globals())

    print('retriev the configuration file')
    file = download_fromStorage(file_key="google/weather-targeting-config.json", bucket_name='xax-configs', client=client,
                                    string=False)
    f = open(file)
    conf = json.load(f)

    temperature_threshold = conf['weather-targeting']['temperature_threshold']
    bid_price = conf['weather-targeting']['bid_price']
    humidex_bins = conf['weather-targeting']['humidex_bins']
    heat_index_bins = conf['weather-targeting']['heat_index_bins']
    windchillindex_bins = conf['weather-targeting']['windchillindex_bins']
    iTimezone = conf['weather-targeting']['timezone']
    dtime_min = conf['weather-targeting']['start_hour_from_now']
    dtime_max = conf['weather-targeting']['end_hour_from_now']
    weather_api_key = conf['weather-targeting']['weather_api_key']

        # -- Seat id and Line item id of the campaing
    seat_id = conf['weather-targeting']['seat_id']
    line_item_id = conf['weather-targeting'][ 'line_item_id'] 
                                 # (https://console.appnexus.com/order-home/line-item/9622000)   #?

        # -- Username and password for calling Appnexus API
    username = conf['weather-targeting']['username']  # appnexus username
    password = conf['weather-targeting']['password']  # appnexus password it should have api access

        # -- Define your model type: Shoule Bonsai Tree be uploaded as Bid modifier or Expected Bid values?
    model_type = conf['weather-targeting']['model_type']  # 'bid' if it is bid; 'bid_modifier' if it is bid modifier

    weather_api_url = conf['weather-targeting']['weather_api_url']
    url_authorisation = conf['weather-targeting']['apn_authorization']
    url_validate = conf['weather-targeting']['apn_validate']
    url_custom_model = conf['weather-targeting']['apn_custom_model']
    url_lineitem = conf['weather-targeting']['apn_lineitem']
    url_lineitem_verify = conf['weather-targeting']['apn_lineitem_verify']

        # -- Model name has to be unique name, if We want to re-run the same code, we must change the modelname.
    Ltime = int(datetime.now(timezone(iTimezone)).timestamp())
    model_name = f"WeatherTargeting_LI{line_item_id}_LnxTime_{Ltime}"
    print('\nThe code starts to run at:', datetime.now(timezone(iTimezone)).strftime('%d-%B-%Y %H:%M:%S'))
    print('Model Name:', model_name)

    T_min = (datetime.now(timezone(iTimezone)) + timedelta(hours=dtime_min)).strftime('%d%b%Y_H%H')
    T_max = (datetime.now(timezone(iTimezone)) + timedelta(hours=dtime_max)).strftime('%d%b%Y_H%H')
    
    # check whether the final output file is there
    output_file =  f'{dtime_max - dtime_min}hours_{T_min}-{T_max}-final.csv'
    f = download_fromStorage(file_key= output_file, bucket_name='xax-weather-targeting',
                             client=client, string=False)
    if  f:
        print('output file is already there, exit the program')
    else: 
        print('waiting for other files in the bucket')
        sleep(420)


        # conbine all the files
        f = f'{dtime_max - dtime_min}hours_{T_min}-{T_max}'
        f1 = f + '-1.csv'
        f2 = f + '-2.csv'
        f3 = f + '-3.csv'

        f1 = download_fromStorage(file_key= f1, bucket_name='xax-weather-targeting',
            client=client, string=False)
        f2 = download_fromStorage(file_key= f2, bucket_name='xax-weather-targeting',
            client=client, string=False)
        f3 = download_fromStorage(file_key= f3, bucket_name='xax-weather-targeting',
            client=client, string=False)

        if bool(f1) & bool(f2) & bool(f3):
            df1 = pd.read_csv(f1, sep=',')
            df2 = pd.read_csv(f2, sep=',')
            df3 = pd.read_csv(f3, sep=',')
            df_forecast = pd.concat([df1, df2, df3], axis=0)

            # add additional columns to the forecast data
            df_weather_index = add_weather_index(df_forecast['temperature'], df_forecast['dewPoint'],
                                                df_forecast['humidity'], df_forecast['windSpeed'],
                                                humidex_bins, heat_index_bins, windchillindex_bins)
            df_user_day_hour = add_user_dayhour(df_forecast['datetime'])
            df_forecast = pd.concat([df_forecast, df_weather_index, df_user_day_hour], axis=1)
            df_forecast = add_bid_value(df_forecast, temperature_threshold, bid_price)
            df_forecast = df_forecast.sort_values(['user_day', 'user_hour', 'city'])

            # save the file
            print('Save weather forecast file and upload')
            print("Tmin_stamp:", T_min, "Tmax_stamp:", T_max, "\n")
            file_name = '/tmp/' + f'{dtime_max - dtime_min}hours_{T_min}-{T_max}-final.csv'
            df_forecast.to_csv(file_name, encoding='utf-8', index=False, sep=',')
            upload_file_from_filename(bucket='xax-weather-targeting', filename=file_name, client=client)

            # create bonsai
            print('Save bonsai tree file and upload')
            bonsai = bonsai_generator(df_forecast, 'city')
            Tstamp = datetime.now().strftime('%d%b%Y_H%H')
            file_name = '/tmp/' + f'BIDs_{Tstamp}.txt'
            a = open(file_name, 'w')
            a.write(bonsai)
            a.close()
            upload_file_from_filename(bucket='xax-weather-targeting', filename=file_name, client=client)

                # upload the bonsai tree
            line_item_response = upload_tree_apn(username, password, model_name, seat_id, model_type, line_item_id, bonsai,
                                                url_authorisation, url_validate, url_custom_model, url_lineitem,
                                                url_lineitem_verify)
        else:
            print('the files from all the previous cloud functions are missing, exit the program')

main()