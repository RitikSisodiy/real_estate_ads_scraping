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
    producer.PushDataList('test',ads)
    # totalads = ''
    # for ad in ads:
    #     totalads+= json.dumps(ad)+"\n"
    # if totalads:
    #     with open('out1.json','a') as file:
    #         file.write(totalads)

def CrawlLeboncoin(parameter):
    headers = {
        # cookie=datadome= 
        'Cookie': 'datadome=eawm5h-VRNtK0hZjHQ18x0Rzkp9YPIC_xzkNOTiKM~Zo_dyGUJBrbWK0cA519MCu7vCs.UHcl_mlxFo55hWPNnHwPzalUrm7jMeo3ram7XzCF7XOsQVRdsZ_Zc0IC93; didomi_token=eyJ1c2VyX2lkIjoiNjcwMjFjZmItMTlhZS00NmY0LWIyMDktM2NiYzdhZTJjNjg4IiwidmVuZG9ycyI6eyJlbmFibGVkIjpbImM6cm9ja3lvdSIsImM6cHVib2NlYW4tYjZCSk10c2UiLCJjOnJ0YXJnZXQtR2VmTVZ5aUMiLCJjOnNjaGlic3RlZC1NUVBYYXF5aCIsImM6Z3JlZW5ob3VzZS1RS2JHQmtzNCIsImM6cmVhbHplaXRnLWI2S0NreHlWIiwiYzpsZW1vbWVkaWEtemJZaHAyUWMiLCJjOnlvcm1lZGlhcy1xbkJXaFF5UyIsImM6bWF5dHJpY3NnLUFTMzVZYW05IiwiYzpzYW5vbWEiLCJjOnJhZHZlcnRpcy1TSnBhMjVIOCIsImM6cXdlcnRpemUtemRuZ0UyaHgiLCJjOmxiY2ZyYW5jZSIsImM6cmV2bGlmdGVyLWNScE1ucDV4IiwiYzpyZXNlYXJjaC1ub3ciLCJjOndoZW5ldmVybS04Vllod2IyUCIsImM6YWRtb3Rpb24iLCJjOnRoaXJkcHJlc2UtU3NLd21IVksiLCJjOmFmZmlsaW5ldCIsImM6aW50b3dvd2luLXFhenQ1dEdpIiwiYzpkaWRvbWkiLCJjOmpxdWVyeSIsImM6YWthbWFpIiwiYzphYi10YXN0eSIsImM6emFub3giLCJjOm1vYmlmeSIsImM6YXQtaW50ZXJuZXQiLCJjOnB1cnBvc2VsYS0zdzRaZktLRCIsImM6aW5mZWN0aW91cy1tZWRpYSIsImM6bWF4Y2RuLWlVTXROcWNMIiwiYzpjbG91ZGZsYXJlIiwiYzppbnRpbWF0ZS1tZXJnZXIiLCJjOmFkdmFuc2UtSDZxYmF4blEiLCJjOnJldGFyZ2V0ZXItYmVhY29uIiwiYzp0dXJibyIsImM6Y2FibGF0b2xpLW5SbVZhd3AyIiwiYzp2aWFudC00N3gyWWhmNyIsImM6dnVibGUtY01DSlZ4NGUiLCJjOmJyYW5jaC1WMmRFQlJ4SiIsImM6c2ZyLU1kcGk3a2ZOIiwiYzphcHBzZmx5ZXItWXJQZEdGNjMiLCJjOmhhc29mZmVyLThZeU1UdFhpIiwiYzpsa3FkLWNVOVFtQjZXIiwiYzpzd2F2ZW4tTFlCcmltQVoiLCJjOmZvcnR2aXNpb24taWU2YlhUdzkiLCJjOmFkaW1vLVBoVVZtNkZFIiwiYzpvc2Nhcm9jb20tRlJjaE5kbkgiLCJjOnJldGVuY3ktQ0xlclppR0wiLCJjOmlsbHVtYXRlYy1DaHRFQjRlayIsImM6YWRsaWdodG5pLXRXWkdyZWhUIiwiYzpyb2NrZXJib3gtZlRNOEVKOVAiXSwiZGlzYWJsZWQiOltdfSwicHVycG9zZXMiOnsiZW5hYmxlZCI6WyJjb29raWVzIiwic2VsZWN0X2Jhc2ljX2FkcyIsImNyZWF0ZV9hZHNfcHJvZmlsZSIsIm1lYXN1cmVfYWRfcGVyZm9ybWFuY2UiLCJtYXJrZXRfcmVzZWFyY2giLCJpbXByb3ZlX3Byb2R1Y3RzIiwic2VsZWN0X3BlcnNvbmFsaXplZF9hZHMiLCJnZW9sb2NhdGlvbl9kYXRhIiwicGVyc29ubmFsaXNhdGlvbm1hcmtldGluZyIsInByaXgiLCJtZXN1cmVhdWRpZW5jZSIsImV4cGVyaWVuY2V1dGlsaXNhdGV1ciJdLCJkaXNhYmxlZCI6W119fQ==;__Secure-InstanceId=a8234aaa-20cb-4088-b959-001b642a40aa;',                   
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

    # response = requests.post('https://api.leboncoin.fr/api/adfinder/v1/search', headers=headers, verify=False,json=parameter)
    time.sleep(3)
    print(response.status)
    if response.status==200:
        res= json.load(response)
        saveAds(res)
        return res
    else:
        print(response.status_code,"some issue do you wanna retry y/n")
        ch = input()
        if ch=="Y" or ch=="y":
            return CrawlLeboncoin(parameter)
        else:
            return False

data = json.load(open('filter.json','r'))
res = CrawlLeboncoin(data)
nextpage = checkNext(res)
while nextpage:
    print(nextpage)
    data['pivot'] = nextpage
    res = CrawlLeboncoin(data)
    nextpage = checkNext(res)