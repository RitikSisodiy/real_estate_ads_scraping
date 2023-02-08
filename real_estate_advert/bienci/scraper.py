import asyncio
from datetime import datetime
import json
import os
import urllib
import sys
import traceback
import requests

from HttpRequest.requestsModules import HttpRequest
from .parser import ParseBienici
from requests_html import AsyncHTMLSession,HTMLSession
import random
from HttpRequest.uploader import AsyncKafkaTopicProducer
try:
    from fetch import fetch,getUserAgent
except:
    from .fetch import fetch,getUserAgent
pageSize = 499
kafkaTopicName = "bienici_data_v1"
commanTopicName = "common-ads-data_v1"

# define your filter here
cpath =os.path.dirname(__file__) or "." 
citys = open(f"{cpath}/finalcitys.json",'r').readlines()
citys = [[json.loads(d)['name'],json.loads(d)['zoneIds'] ] for d in citys]
citys.sort()
lastpin ,lastpage = None,1
if lastpin:start = False
else:start = True
def getFilterUrl(Filter,page=None):
    if page:
        Filter['from'] = Filter['size']*page +1
    Filter = json.dumps(Filter)
    
    Filter =  {"filters":Filter}
    Filter = urllib.parse.urlencode(Filter)
    url  = f"https://www.bienici.com/realEstateAds.json?{Filter}&extensionType=extendedIfNoResult"
    return url
def GetLast(val):
    try:
        return val[len(val)-1]
    except:
        return None
def getFirst(val):
    try:
        return val[0] 
    except:
        return None
def syncfetch(session,url,Json=False,**kwargs):
    kwargs['headers'] = {
                "user-agent":getUserAgent(),
                }
    res = session.get(url,**kwargs)
    if res.status_code == 200:
        if Json:return res.json()
        else:return res
    else:
        print(res.status_code)
        print(url)
        return syncfetch(session,url,Json=Json,**kwargs)
def getTotalResult(session,parameter):
    parameter['sortBy'],parameter['sortOrder'],parameter["size"],parameter["from"] = "price","desc",1,0
    url = getFilterUrl(parameter)
    res = syncfetch(session,url,Json=True)
    totalres = res.get("total")
    if totalres:return totalres
    else:return 0
def getMaxPrize(session,parameter):
    parameter['sortBy'],parameter['sortOrder'],parameter["size"] = "price","desc",1
    url = getFilterUrl(parameter)
    res = syncfetch(session,url,Json=True)
    totalres = res.get("realEstateAds")[0].get("price")
    if totalres:return totalres
    else:return 0
def readGenFilter():
    try:
        with open("generatedPrizeFilter.json",'r') as file:
            data = json.load(file)
        return data
    except:return {}
def writeGenFilter(key,value):
    prev = readGenFilter()
    prev[key]=value
    with open("generatedPrizeFilter.json",'w') as file:
        file.write(json.dumps(prev))
def genFilter(parameter,typ):
    parameter["filterType"] = typ
    session = requests.session()
    dic = parameter
    totalresult = getTotalResult(session,dic)
    acres = totalresult
    # dic['recherche[produit]']=typ
    iniinterval = [0,37761]
    maxprize = getMaxPrize(session,dic)
    maxresult = 2400
    filterurllist = ""
    finalresult = 0
    nooffilter = 0
    retrydic = {iniinterval[0]:0}
    while iniinterval[1]<=maxprize:
        dic['minPrice'],dic['maxPrice'] = iniinterval
        totalresult = getTotalResult(session,dic)
        if (totalresult!=0 and maxresult-totalresult<=1400 and maxresult-totalresult>0) or (retrydic[iniinterval[0]]>10 and totalresult>0 and totalresult<maxresult):
            # print("condition is stisfy going to next interval",totalresult)
            # print(iniinterval,">apending")
            filterurllist += json.dumps(iniinterval) + "/n/:"
            FetchFilter(dic)
            print("going to next")
            iniinterval[0] = iniinterval[1]+1
            iniinterval[1] = iniinterval[0]+int(iniinterval[0]/2)
            finalresult +=totalresult
            retrydic = {iniinterval[0]:0}
            nooffilter +=1
        elif maxresult-totalresult> pageSize:
            # print("elif 1")
            last = 10
            iniinterval[1] = iniinterval[1] + int(iniinterval[1]/last)
        elif totalresult == 0:
            # print("elif 1",iniinterval)
            last = 10
            iniinterval[1] = iniinterval[1] + int(iniinterval[1]/last)
        elif totalresult>maxresult:
            # print("elif 2",iniinterval)
            last = -5
            dif = iniinterval[1]-iniinterval[0]
            iniinterval[1] = iniinterval[1] + int(dif/-2) 
            if iniinterval[0]>iniinterval[1]:
                iniinterval[1] = iniinterval[0]+10
        retrydic[iniinterval[0]] +=1
        sys.stdout.write(f"\r{totalresult}-{maxresult}::::{acres} of  {finalresult}==>{iniinterval} no of filter {nooffilter}")
        sys.stdout.flush()
        # print(totalresult,"-",maxresult,"::::",acres ," of ", finalresult,"==>",iniinterval)
    filterurllist+=json.dumps(iniinterval)
    finalresult +=totalresult
    print(finalresult,acres)
    filterurllist = [json.loads(query) for query in filterurllist.split("/n/:")]
    writeGenFilter(typ,filterurllist)

asyncpage = 10
# change the url of filter data to scrap all the ads
# async def main():
#     tasks = []
#     count = 0
#     global start
#     global lastpin
#     global lastpage
#     session = AsyncHTMLSession()
#     producer = AsyncKafkaTopicProducer()
#     # await producer.statProducer()
#     # session.proxies.update({"http": "socks5://218.1.142.41:57114", "https": "socks5://218.1.142.41:57114"})
#     print(lastpin, "this ", start)
#     Filter = {
#     "size":pageSize,
#     "from":0,
#     "showAllModels":False,
#     "filterType":"buy",
#     "propertyType":["house","flat"],
#     "newProperty":False,
#     "sortBy":"relevance",
#     "sortOrder":"desc",
#     "onTheMarket":[True],
#     }
#     for pin,zonid in citys:
#         if lastpin == pin:
#             print("this is last pin")
#             start = True 
#         if start:
#             count +=1
#             Filter['zoneIdsByTypes']['zoneIds'] = zonid
#             baseurl = getFilterUrl(Filter)
#             tasks.append(asyncio.ensure_future(GetAllPages(baseurl,session,first=True,Filter=Filter,producer=producer)))
#             if len(tasks) == asyncpage or count == len(citys):
#                 await asyncio.gather(*tasks)
#                 t=random.randint(5,15)
#                 print(f"wait for {t} seconds")
#                 await asyncio.sleep(t)
#                 tasks=[]
#     await asyncio.gather(*tasks)
#     await producer.stopProducer()

def saveRealstateAds(ads,**kwargs):
    producer = kwargs.get("producer")
    producer.PushDataList(kafkaTopicName,ads)
    ads = [ParseBienici(ad) for ad in ads]
    producer.PushDataList(commanTopicName,ads)

    # allads = ''
    # for ad in ads:
    #     allads+= json.dumps(ad)+"\n"
    # with open("output/output.json",'a') as file:
    #     file.write(allads)

def GetAllPages(baseurl,session,first=False,Filter=None,save=True,**kwargs):
    print(Filter)
    r= fetch(baseurl,session,Json=True)
    if r:
        # print(baseurl,"-200")
        print(f"from===========>{r['from']}")
        if save:
            saveRealstateAds(r['realEstateAds'],**kwargs)
        else:
            return r["realEstateAds"]
        if first:
            totalpage = r['total']/Filter['size']
            totalpage = int(totalpage)+1 if totalpage>int(totalpage) else int(totalpage)
            # print(len(r["realEstateAds"]),r['total'],"tis is ")
            tasks =[]
            print(totalpage,r['total'],Filter['size'])
            # input("chekd the pages")
            for i in range(1,totalpage):
                Filter['from'] += Filter['size']
                baseurl = getFilterUrl(Filter,page=i)
                GetAllPages(baseurl,session,first=False,Filter=Filter,**kwargs)
            # await asyncio.gather(*tasks)

def FetchFilter(filters):
    filters['size'] = 400
    headers  = {
                "user-agent":getUserAgent(),
    }
    session = HttpRequest(True,'https://www.bienici.com/realEstateAds.json?filters={"size":"1"}',{},headers,{},False,cpath,1,10)
    try:
        baseurl = getFilterUrl(filters)
        producer = AsyncKafkaTopicProducer()
        # await producer.statProducer()
        GetAllPages(baseurl,session,first=True,Filter=filters,producer=producer)
    finally:
        session.__del__()
    # await producer.stopProducer()

def getLastUpdates():
    try:
        with open(f'{cpath}/lastUpdate.json','r') as file:
            updates = json.load(file)
    except:
        return {}
    return updates
def getTimeStamp(strtime):
    formate = '%Y-%m-%dT%H:%M:%S.%fZ'
    #1970-01-01T00:00:00.000Z
    t = datetime.strptime(strtime,formate)
    return t.timestamp()
def GetAdUpdate(ad):
    nowtime  = datetime.now()
    update = {
            "timestamp": nowtime.timestamp(),
            "lastupdate": getTimeStamp(ad['publicationDate']),
            "lastadId": ad["id"],
        }
    return update
def CreatelastupdateLog(session,typ):
    updates = getLastUpdates()
    if typ == "sale" or typ=="buy":
        typ = "buy"
    else:
        typ = "rent"
    param  = {
    "size":pageSize,
    "from":0,
    "showAllModels":False,
    "propertyType":["house","flat"],
    "filterType":typ,
    "newProperty":False,
    "page":1,
    "sortBy":"publicationDate",
    "sortOrder":"desc",
    "onTheMarket":[True],
    }
    url = getFilterUrl(param)
    print("fetching url :", url)
    d = fetch(url,session,Json=True)

    try:
        ad = d["realEstateAds"][0]
        latupdate = GetAdUpdate(ad)
        # print(latupdate)
        updates.update({typ:latupdate})
    except Exception as e:
        traceback.print_exc()
        print("execption ======>" , e)
    # lastupdate = json.load(open(f'{cpath}/lastUpdate.json','r'))
    print(updates)
    with open(f'{cpath}/lastUpdate.json','w') as file:
        file.write(json.dumps(updates))
def asyncUpdateBienci():
    headers  = {
                "user-agent":getUserAgent(),
    }
    session = HttpRequest(True,'https://www.bienici.com/realEstateAds.json?filters={"size":"1"}',{},headers,{},False,cpath,1,10)
    res = ""
    try:
        producer = AsyncKafkaTopicProducer()
        # await producer.statProducer()
        param  = {
        "size":60,
        "from":0,
        "showAllModels":False,
        "propertyType":["house","flat"],
        "newProperty":False,
        "page":1,
        "sortBy":"publicationDate",
        "sortOrder":"desc",
        "onTheMarket":[True],
        }
        updates = getLastUpdates()
        print(updates)
        if updates:
            CreatelastupdateLog(session,'rent')
            CreatelastupdateLog(session,'buy')
            for key,val in updates.items():
                param.update({"filterType":key})
                updated = False
                page= 1
                while not updated and page*param['size']<2400:
                    url = getFilterUrl(param,page=page)
                    r = fetch(url,session,Json=True)
                    ads = r['realEstateAds']
                    adslist = []
                    for ad in ads:
                        adtime = getTimeStamp(ad["publicationDate"])
                        print(f"{val['lastupdate']}<{adtime} = ",val['lastupdate']<getTimeStamp(ad["publicationDate"]))
                        if val['lastupdate']<getTimeStamp(ad["publicationDate"]):
                            adslist.append(ad)
                        else:
                            msg = f"{len(adslist)} {key} new ads scraped"
                            res += " "+ msg
                            print(msg)
                            updated = True
                            break
                    producer.PushDataList(kafkaTopicName,adslist)
                    page +=1
            # producer.stopProducer()
            
        else:
            CreatelastupdateLog(session,'rent')
            CreatelastupdateLog(session,'buy')
    finally:
        session.__del__()
    return res
def CheckId(id):
    session = HTMLSession()
    url = f"https://www.bienici.com/realEstateAd.json?id={id}"
    res = fetch(url,session,Json=True)
    return bool(res)
def UpdateBienci():
    return asyncUpdateBienci()

def main_scraper(payload):
    # asyncio.run(main())
    if payload.get("real_state_type") == "Updated/Latest Ads":
        res = UpdateBienci()
        return res
    param  = {
    "size":pageSize,
    "from":0,
    "showAllModels":False,
    "filterType":"buy",
    "propertyType":["house","flat"],
    "newProperty":False,
    "page":1,
    "sortBy":"relevance",
    "sortOrder":"desc",
    "onTheMarket":[True],
    }
    genFilter(param,typ="buy")
    genFilter(param,typ="rent")
if __name__ == "__main__":
    # asyncio.run(main())
    param  = {
    "size":pageSize,
    "from":0,
    "showAllModels":False,
    "filterType":"buy",
    "propertyType":["house","flat"],
    "newProperty":False,
    "page":1,
    "sortBy":"relevance",
    "sortOrder":"desc",
    "onTheMarket":[True],
    }
    genFilter(param,typ="buy")
    genFilter(param,typ="rent")
