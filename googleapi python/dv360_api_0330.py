def request_post(url, data, headers=None):
    if type(data) is str:
        http = urllib3.PoolManager()
        if headers is None:
            r = http.request('POST', url,
                             headers={'Content-Type': 'application/json'},
                             body=data,
                             verify=False)
        else:
            r = http.request('POST', url,
                             headers=headers,
                             body=data,
                             verify=False)
    else:
        raise print("Data in post request must be string")

    return (r)

def request_get(url, data, headers=None):
    if type(data) is str:
        http = urllib3.PoolManager()
        if headers is None:
            r = http.request('GET', url,
                             headers={'Content-Type': 'application/json'},
                             body=data,
                             verify=False)
        else:
            r = http.request('GET', url,
                             headers=headers,
                             body=data,
                             verify=False)
    else:
        raise print("Data in get request must be string")

    return (r)

def request_delete(url, data, headers=None):
    if type(data) is str:
        http = urllib3.PoolManager()
        if headers is None:
            r = http.request('DELETE', url,
                             headers={'Content-Type': 'application/json'},
                             body=data,
                             verify=False)
        else:
            r = http.request('DELETE', url,
                             headers=headers,
                             body=data,
                             verify=False)
    else:
        raise print("Data in get request must be string")

    return (r)

def request_patch(url, data, headers=None):
    if type(data) is str:
        http = urllib3.PoolManager()
        if headers is None:
            r = http.request('PATCH', url,
                             headers={'Content-Type': 'application/json'},
                             body=data,
                             verify=False)
        else:
            r = http.request('PATCH', url,
                             headers=headers,
                             body=data,
                             verify=False)
    else:
        raise print("Data in get request must be string")

    return (r)

def read_json(file_path):
    with open(partner_path) as json_file_partner:
        r = json.load(json_file_partner)
        ordId = r["partners"]["partner_ids"][0]
        xaxisId = r["partners"]["partner_ids"][1]
    with open(url_path) as json_file_url:
        u = json.load(json_file_url)
    return (r, u)

# get advertiser ids
def dv360_advertiserId():
    adv_url = u["urls"]["list"]["advertiser"]
    x = request_get(adv_url, data=data, headers=None)
    adv_ids=[]
    for id in x["advertisers"]:
        adv_ids.append(id["advertiserId"])
    return (adv_ids)

# get campaign ids, insertion order ids, line item ids
def dv360_ids(advertiserId):
    li_url = u["urls"]["list"]["lineItem"]
    x = request_get(li_url, data=data, headers=None)
    campaign_ids=[]
    io_ids=[]
    li_ids=[]
    for id in x["lineItems"]:
        campaign_ids.append(id["campaignId"])
        io_ids.append(id["insertionOrderId"])
        li_ids.append(id["lineItemId"])
        
    return (campaign_ids, io_ids,li_ids)

# get negativeKeyword List ids
def dv360_negativeKwListId(advertiserId):
    kw_list_url = u["urls"]["list"]["negativeKeywordLists"]
    x = request_get(kw_list_url, data=data, headers=None)
    kw_list_ids=[]
    for id in x["negativeKeywordLists"]:
        kw_list_ids.append(id["negativeKeywordListId"])
    
    return (kw_list_ids)

#create campaigns, insertion orders, line items, negativeKeywordLists and negativeKeywords
def dv360_create(advertiser_id):
    campaign_url = u["urls"]["create"]["campaign"]
    io_url = u["urls"]["create"]["insertionOder"]
    li_url = u["urls"]["create"]["lineItem"]
    kw_list_url = u["urls"]["create"]["negativeKeywordLists"]
    kw_url = u["urls"]["create"]["negativeKeywords"]
    
    campaign = request_post(campaign_url, data = data, headers=None)
    io = request_post(io_url, data = data, headers=None)
    li = request_post(li_url, data = data, headers=None)
    kw_list = request_post(kw_list_url, data = data, headers=None)
    kw = request_post(kw_url, data = data, headers=None)
    
    return (campaign, io, li, kw_list, kw)

#delete campaigns, insertion orders, line items, negativeKeywordLists and negativeKeywords
def dv360_delete(advertiser_id, insertionOrderId, campaignId, lineItemId, negativeKeywordListId, keywordValue):
    campaign_url = u["urls"]["delete"]["campaign"]
    io_url = u["urls"]["delete"]["insertionOder"]
    li_url = u["urls"]["delete"]["lineItem"]
    kw_list_url = u["urls"]["delete"]["negativeKeywordLists"]
    kw_url = u["urls"]["delete"]["negativeKeywords"]
    
    campaign = request_delete(campaign_url, data = data, headers=None)
    io = request_delete(io_url, data = data, headers=None)
    li = request_delete(li_url, data = data, headers=None)
    kw_list = request_delete(kw_list_url, data = data, headers=None)
    kw = request_delete(kw_url, data = data, headers=None)
    
    return (campaign, io, li, kw_list, kw)

#get campaigns, insertion orders, line items, negativeKeywordLists and negativeKeywords
def dv360_get(advertiser_id, insertionOrderId, campaignId, lineItemId, negativeKeywordListId):
    campaign_url = u["urls"]["get"]["campaign"]
    io_url = u["urls"]["get"]["insertionOder"]
    li_url = u["urls"]["get"]["lineItem"]
    kw_list_url = u["urls"]["get"]["negativeKeywordLists"]
    kw_url = u["urls"]["get"]["negativeKeywords"]
    
    campaign = request_get(campaign_url, data = data, headers=None)
    io = request_get(io_url, data = data, headers=None)
    li = request_get(li_url, data = data, headers=None)
    kw_list = request_get(kw_list_url, data = data, headers=None)
    kw = request_get(kw_url, data = data, headers=None)
    
    return (campaign, io, li, kw_list, kw)

#update existing campaigns, insertion orders, line items, negativeKeywordLists and negativeKeywords
def dv360_get(advertiser_id, insertionOrderId, campaignId, lineItemId, negativeKeywordListId):
    campaign_url = u["urls"]["patch"]["campaign"]
    io_url = u["urls"]["patch"]["insertionOder"]
    li_url = u["urls"]["patch"]["lineItem"]
    kw_list_url = u["urls"]["patch"]["negativeKeywordLists"]

    campaign = request_patch(campaign_url, data = data, headers=None)
    io = request_patch(io_url, data = data, headers=None)
    li = request_patch(li_url, data = data, headers=None)
    kw_list = request_patch(kw_list_url, data = data, headers=None)
    
    return (campaign, io, li, kw_list, kw)
