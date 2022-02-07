def request_get(url, headers = None):
    http = urllib3.PoolManager()
    if headers is None:
        r = http.request('GET', url)
    else:
        r = http.request('GET', url, headers = headers)
    return(r)
    
def get_dbm_user(token):
  url = 'https://displayvideo.googleapis.com/v1/users'
  x = request_get(url = url,  headers={"Authorization": "Bearer " + token})
  return(x)

def get_dbm_io(advertiser_id, token):
  url = 'https://displayvideo.googleapis.com/v1/advertisers/' + advertiser_id + '/insertionOrders'
  x = request_get(url = url,  headers={"Authorization": "Bearer " + token})
  return (x)

def get_dbm_LI(advertiser_id, token):
  url = 'https://displayvideo.googleapis.com/v1/advertisers/' + advertiser_id + '/lineItems'
  x = request_get(url = url,  headers={"Authorization": "Bearer " + token})
  return (x)

def get_dbm_campaigns(advertiser_id, token):
  url = 'https://displayvideo.googleapis.com/v1/advertisers/' + advertiser_id + '/campaigns
  x = request_get(url = url,  headers={"Authorization": "Bearer " + token})
  return (x)

def get_dbm_kw(advertiser_id, token):
  url = 'https://displayvideo.googleapis.com/v1/advertisers/' + advertiser_id + '/negativeKeywordLists'
  x = request_get(url = url,  headers={"Authorization": "Bearer " + token})
  return (x)

def get_dbm_kw_list(advertiser_id, token):
  get_dbm_kw(url = 'https://displayvideo.googleapis.com/v1/advertisers/' + advertiser_id + '/negativeKeywordLists', token)
  for id in x['negativeKeywordLists']:
    kw_id = id['negativeKeywordListId']
  kw_url = 'https://displayvideo.googleapis.com/v1/advertisers/' + advertiser_id + '/negativeKeywordLists/' + kw_id +'/negativeKeywords'
  a = request_get(url = kw_url,  headers={"Authorization": "Bearer " + token})
  return (a)