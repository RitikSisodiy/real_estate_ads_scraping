import urllib.request


import requests
import json
from datetime import datetime
import time
try:
    from uploader import AsyncKafkaTopicProducer
except:
    from .uploader import AsyncKafkaTopicProducer

producer = AsyncKafkaTopicProducer()
def now_time_int():
    dateobj = datetime.now()
    total = int(dateobj.strftime('%S'))
    total += int(dateobj.strftime('%M')) * 60
    total += int(dateobj.strftime('%H')) * 60 * 60
    total += (int(dateobj.strftime('%j')) - 1) * 60 * 60 * 24
    total += (int(dateobj.strftime('%Y')) - 1970) * 60 * 60 * 24 * 365
    return total
def checkNext(res):
    try:
        next = res['pivot']
        return next
    except:
        return False
    page = next.get("page_number")
    if page:
        next = {"es_pivot":now_time_int(),"page_number":page}
        return next
    else:
        return False
def saveAds(res):
    ads = res.get('ads')
    # producer.PushDataList('leboncoin-data_v1',ads)
    # totalads = ''
    # for ad in ads:
    #     totalads+= json.dumps(ad)+"\n"
    # if totalads:
    #     with open('out1.json','a') as file:
    #         file.write(totalads)

def CrawlLeboncoin(parameter):
    headers = {
        # cookie=datadome= 
        'Cookie': 'datadome=gQo17p3.n6.z_5CDCp1M3PHjv6n3bYZs-V3w3z5vl~iwy9pW3660qnY3q2M_.hd1tNl_XoQIp3-fVbps4f4YItCMgpdMBk4giF3X74gyh7cxxFzSA-Vf_sNtHF9Dd3d;',                   
        'X-LBC-CC': '7',
        'Accept': 'application/json,application/hal+json',
        'User-Agent': 'LBC;Android;7.1.2;ASUS_Z01QD;phone;a3ab60b8d97f0f1a;wifi;5.69.0;569000;0',
        'Content-Type': 'application/json; charset=UTF-8',
        'Host': 'api.leboncoin.fr',
        'Connection': 'Keep-Alive',
        # 'Accept-Encoding': 'gzip',
    }
    jsondata = json.dumps(parameter)
    jsondataasbytes = jsondata.encode('utf-8')
    req = urllib.request.Request("https://api.leboncoin.fr/api/adfinder/v1/search",headers=headers)
    req.add_header('Content-Length', len(jsondataasbytes))
    response = urllib.request.urlopen(req, jsondataasbytes)
    # response = opener.open(req, jsondataasbytes)

    # response = requests.post('https://api.leboncoin.fr/api/adfinder/v1/search', headers=headers, verify=False,json=parameter)
    time.sleep(5)
    print(response.status)
    if response.status==200:
        res= json.load(response)
        saveAds(res)
        return res
    else:
        print(response.status_code,"some issue do you wanna retry y/n")
        # ch = input()
        # if ch=="Y" or ch=="y":
        time.sleep(300)
        return CrawlLeboncoin(parameter)
        # else:
        #     return False

data = json.load(open('filter.json','r'))
res = CrawlLeboncoin(data)
nextpage = checkNext(res)
while nextpage:
    print(nextpage)
    data['pivot'] = nextpage
    res = CrawlLeboncoin(data)
    nextpage = checkNext(res)