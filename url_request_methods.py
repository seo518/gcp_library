import urllib3


## GET REQUEST
def request_get(url, headers = None):
    http = urllib3.PoolManager()
    if headers is None:
        r = http.request('GET', url)
    else:
        r = http.request('GET', url, headers=headers)
    return(r)

## POST REQUEST
def request_post(url, data, headers = None):
    if type(data) is str:
        http = urllib3.PoolManager()
        if headers is None:
            r = http.request('POST', url,
                             headers={'Content-Type': 'application/json'},
                             body=data)
        else:
            r = http.request('POST', url,
                             headers=headers,
                             body=data)
    else:
        raise print("Data in post request must be string")
        
    return(r)