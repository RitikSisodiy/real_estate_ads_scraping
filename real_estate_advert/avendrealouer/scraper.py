from datetime import datetime,timedelta
from saveLastChaeck import saveLastCheck
import traceback,concurrent.futures
import asyncio,requests
import os
from requests_html import HTMLSession
import json,re,time
from HttpRequest.uploader import AsyncKafkaTopicProducer
from HttpRequest.requestsModules import HttpRequest
import settings
from .parser import ParseAvendrealouer
website = "avendrealouer.fr"
kafkaTopicName = settings.KAFKA_AVENDREALOUER
commonTopicName = settings.KAFKA_COMMON_PATTERN
commonIdUpdate = f"activeid-{website}"
s= HTMLSession()
pagesize  = 100 # maxsize is 100
cpath =os.path.dirname(__file__) or "."
url = "https://ws-web.avendrealouer.fr/realestate/properties/"
ajencyurl = "https://ws-web.avendrealouer.fr/common/accounts/?id="
gparams = {
        "typeIds":"2,3,6,7,19",
        "size":25,
        "transactionIds":"vente",
        "from":0,
        "sorts[0].Name":"_newest",
        "sorts[0].Order":"desc"

        }
headers = {
    "Accept": "*/*",
    "Authorization": "Basic ZWQ5NjUwYTM6Y2MwZDE4NTRmZmE5MzYyODE2NjQ1MmQyMjU4ZWMxNjI=",
    "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 11; sdk_gphone_x86 Build/RSR1.201013.001)",
    "Host": "ws-web.avendrealouer.fr",
    "Connection": "Keep-Alive",
    "Accept-Encoding": "gzip",
}
def fetch(session,url,params = None,method="get",retry=0,**kwargs):
    # if params:
    #     query_string = urllib.parse.urlencode( params )
    #     url += "?"+query_string 
    if retry>3:
        return None
    retry+=1
    try:
        res = session.fetch(url,headers = headers,params=params)
    except Exception as e:
        time.sleep(3)
        return fetch(session,url,params,method,retry=retry,**kwargs)
    if res and res.status_code==200:
        response = res.json()
    else:
        return fetch(session,url,params,method,retry=retry,**kwargs)
    return response


def getTimeStamp(strtime):
    formate = '%Y-%m-%d %H:%M:%S'
    # 2022-09-14 20:00:00
    t = datetime.strptime(strtime,formate)
    return t.timestamp()
def savedata(resjson,**kwargs):
    resstr = ''
    ads = resjson["items"]
    producer = kwargs["producer"]
    if kwargs.get("onlyid"):
        now = datetime.now()
        ads = [{"id":"aven"+str(ad.get("id")), "last_checked": now.isoformat(),"available":True} for ad in ads]
        producer.PushDataList_v1(commonIdUpdate,ads)
    else:
        producer.PushDataList(kafkaTopicName,ads)
        ads = [ParseAvendrealouer(ad) for ad in ads]
        producer.PushDataList(commonTopicName,ads)
        print("saved")
    # for ad in ads:
    #     resstr += json.dumps(ad)+"\n"
    # with open("output.json",'a') as file:
    #     file.write(resstr)
    # print('saved data')
def startCrawling(session,param,**kwargs):
    # for param in filterParamList:
        param["size"] = pagesize
        data = fetch(session,url,param)
        # data= json.load(res)
        savedata(data,**kwargs)
        totalres = data.get("count") or 0
        totalpage = totalres/pagesize
        totalpage = int(totalpage) if totalpage==int(totalpage) else int(totalpage)+1
        print(totalres,param)
        print(totalpage,"this is total pages")
        filterlist = []
        if totalpage > 7:
            param.update({"sorts[0].Order":"desc"})
            filterlist.append((7,param))
            param.update({"sorts[0].Order":"asc"})
            filterlist.append(((totalpage-7),param))
        else:
            filterlist.append((totalpage,param))
        start = 2
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as excuter:
            futures = []
            for totalpage,param in filterlist:
                # print(param)
                # await asyncio.sleep(5)
                # tasks = []
                for i in range(start,totalpage+1):
                    param.update({'currentPage': i})
                    parmacopy = param.copy()
                    # print(param)
                    futures.append(excuter.submit(parstItems,session,parmacopy,page=i,**kwargs))
                    # tasks.append(asyncio.ensure_future(parstItems(session,param,page=i,**kwargs)))
                # await asyncio.gather(*tasks)
                start=1 
            for f in futures:print(f) 
        # totaldata = 0
        # for d in data:
        #     results = len(d['feed']["row"])
        #     print(results,"this is len of rows")
        #     totaldata += results
        #     await savedata(d,**kwargs)
        # print(totaldata)
def getTotalResult(session,params,url):
    totalres = {
        'size':1,
    }
    params.update(totalres)
    r = fetch(session,url,params=params)
    return int(r.get("count") or 0)
def getMax(session,params,url,max="price"):
    # dic,burl = GetUrlFilterdDict(url)
    # dic["ajaxAffinage"] = 0
    # dic["ddlTri"] = "prix_seul"
    # dic["ddlOrd"] = "desc"
    prizefilter = {
        "sorts[0].Name":max,
        "size":1,
        "from":0,
        "sorts[0].Order":"desc"
    }
    params.update(prizefilter)
    r = fetch(session,url,params=params)
    print(params)
    try:
        prize = r["items"][0][max]
    except:prize = 0
    return float(prize)


# def getFilter(session,params,producer,onlyid=False,low="price.gte",max='price.lte'):
#     dic,baseurl = params , "https://ws-web.avendrealouer.fr/realestate/properties/"
#     print("genrating filters")
#     # url = getUrl(baseurl,dic)
#     # url = baseurl
#     maxresult = 1400
#     try:
#         del dic[low]
#         del dic[max]
#     except:pass
#     totalresult =getTotalResult(session,dic,baseurl)
#     acres = totalresult
#     fetchedresult = 0
#     iniinterval = [0,1000]
#     finalresult = 0
#     filterurllist = []
#     maxprice = getMaxPrize(session,params,baseurl)
#     if totalresult>=maxresult:
#         while iniinterval[1]<=maxprice:
#             print(iniinterval)
#             dic[low],dic[max] = iniinterval
#             totalresult = getTotalResult(session,dic,baseurl)
#             if totalresult <= 1400 and totalresult>0:
#                 print("condition is stisfy going to next interval")
#                 print(iniinterval,">apending")
#                 filterurllist.append(dic.copy())
#                 # filterurllist+=json.dumps(dic)+":\n"
#                 # startCrawling(session,dic,producer=producer,onlyid=onlyid)
#                 # print(filterurllist)
#                 iniinterval[0] = iniinterval[1]+1
#                 iniinterval[1] = iniinterval[0]+int(iniinterval[0]/2)
#                 finalresult +=totalresult
#             elif(acres<=finalresult):break
#             elif iniinterval[1]-iniinterval[0] <=2 and totalresult>maxresult and low=="price.gte":
#                 finalresult +=totalresult
#                 getFilter(session,dic.copy(),producer,onlyid,"surface.gte","surface.lte")
#             elif maxresult-totalresult> 1400:
#                 # print("elif 1")
#                 last = 10
#                 iniinterval[1] = iniinterval[1] + int(iniinterval[1]/last)
#             elif totalresult == 0:
#                 # print("elif 1",iniinterval)
#                 last = 10
#                 iniinterval[1] = iniinterval[1] + int(iniinterval[1]/last)
#             elif totalresult>maxresult:
#                 # print("elif 2",iniinterval)
#                 last = -5
#                 dif = iniinterval[1]-iniinterval[0]
#                 iniinterval[1] = iniinterval[1] + int(dif/-2) 
#                 if iniinterval[0]>iniinterval[1]:
#                     iniinterval[1] = iniinterval[0]+10
#             sys.stdout.write(f"\r{totalresult}-{maxresult}::::{acres} of  {finalresult}==>{iniinterval} no of filter")
#             sys.stdout.flush()
#         print(iniinterval,">apending")
#         startCrawling(session,dic,producer=producer,onlyid=onlyid)
#         finalresult +=totalresult
#     # finallsit = [json.loads(d) for d in filterurllist.split(":\n")]
#     # print(finallsit)
#     # paramslist = []
#     # for par in finallsit:
#     #     params['filters[P5M0]'],params['filters[P5M1]'] = par
#     #     paramslist.append(params)
#     print(f"total result is : {acres} filtered result is: {finalresult}")
#     # print(filterurllist)
#     # time.sleep(10)
#     return 0
def getFilter(session,params,producer,onlyid=False,low="price.gte",max='price.lte'):
    dic,baseurl = params.copy() , "https://ws-web.avendrealouer.fr/realestate/properties/"
    # print("genrating filters")
    # url = getUrl(baseurl,dic)
    # url = baseurl
    maxresult = 1400
    try:
        # del dic[low]
        # del dic[max]
        if low is not"price.gte":dic[low] = 0
    except:pass
    print("fidc",dic)
    totalresult =getTotalResult(session,dic,baseurl)
    acres = totalresult
    fetchedresult = 0
    if low=="price.gte":
        # iniinterval =[98976, 98989]
        iniinterval = [0,1000]
        maxprice = getMax(session,params,baseurl)
    else:
        maxprice = getMax(session,params,baseurl,max="surface")
        iniinterval =[0,1]
    # iniinterval = [0,1000]
    finalresult = 0
    filterurllist = []
    
    while iniinterval[1]<=maxprice:
        # print(iniinterval)
        dic[low],dic[max] = iniinterval
        totalresult = getTotalResult(session,dic,baseurl)
        if totalresult <= 1400 and totalresult>0:
            filterurllist.append(dic.copy())
            startCrawling(session,dic,producer=producer,onlyid=onlyid)
            # filterurllist+=json.dumps(dic)+":\n"
            # print(filterurllist)
            iniinterval[0] = iniinterval[1]+1
            iniinterval[1] = iniinterval[0]+int(iniinterval[0]/2)
            finalresult +=totalresult
        elif acres-finalresult<700 and acres-finalresult>0:
            print(maxresult, acres , maxresult-acres)
            # input()
            # print("elif 1")
            last = 10
            iniinterval[1] = maxprice
        elif totalresult == 0:
            # print("elif 1",iniinterval)
            last = 10
            iniinterval[0] = iniinterval[1]
            iniinterval[1] = iniinterval[0] + (int(iniinterval[1]/last) or 1)
            # iniinterval[0] = iniinterval[1] 
            # iniinterval[1] +=1
        elif iniinterval[1]-iniinterval[0] <=2 and totalresult>maxresult and low=="price.gte":
            finalresult +=totalresult
            getFilter(session,dic.copy(),producer,onlyid,"surface.gte","surface.lte")
            iniinterval[0] = iniinterval[1] 
            iniinterval[1]+=1 
        elif totalresult>maxresult:
            # print("elif 2",iniinterval)
            last = -5
            dif = iniinterval[1]-iniinterval[0]
            iniinterval[1] = iniinterval[1] + int(dif/-2) 
            if iniinterval[0]>iniinterval[1]:
                iniinterval[1] = iniinterval[0]+1
        # retrydic[iniinterval[0]] +=1
        print(f"\r{totalresult}-{maxresult}::::{acres} of  {finalresult}==>{iniinterval} {maxprice} {low} no of filter",end="")
        # sys.stdout.flush()
    # print(iniinterval,">apending")
    filterurllist.append(dic.copy())
    # finalresult +=totalresult
    print(filterurllist)
    return filterurllist

def parstItems(session,param,page=None,save=True,**kwargs):
    if page:
        param.update({"from":((page-1)*pagesize)})
    data = fetch(session,url,param)
    # print(param['p'])
    # data = json.load(res)
    if save:
        savedata(data,**kwargs)
    return data
def CheckId(id):
    
    furl  = f"https://ws-web.avendrealouer.fr/realestate/properties/?id={id}"
    # print(furl)
    try:
        r = requests.get(furl,headers=headers)
        if r.status_code==200:
            return True
        else:return False
    except:return False
def main(adsType = "",onlyid=False):
    params = gparams.copy()
    try:
        session = HttpRequest(True,'https://ws-web.avendrealouer.fr/',headers,{},{},False,cpath,1,10)
        # catid info
        # vente is for  Vente immobilier 
        # location is for Location immobilier
        if adsType == "rental":
            catid = "2"
        else:
            catid = "1,3"
        params["transactionIds"] = catid
        # filterParamList = [*getFilter(param) for param in params
        # async with aiohttp.ClientSession() as session:
        CreatelastupdateLog(session,adsType)
        producer = AsyncKafkaTopicProducer()
        flist = [2,3,6,7,19]
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as excuter:
            futures = []
            for f in flist:
                params.update({"typeIds":f})
                paramscopy = params.copy()
                futures.append(excuter.submit(getFilter,session,paramscopy,producer,onlyid=onlyid))
            for f in futures:print(f)
    finally:
        session.__del__()
    # await startCrawling(session,filterParamList,producer=producer)
    # await producer.stopProducer()
def main_scraper(payload):
    adtype = payload["real_state_type"]
    if adtype == "Updated/Latest Ads":
        UpdateParuvendu()
    elif payload.get("real_state_type") == "deletedCheck":
        rescrapActiveId()
    else:
        main(adtype)
def getLastUpdates():
    try:
        with open(f'{cpath}/lastUpdate.json','r') as file:
            updates = json.load(file)
    except:
        return {}
    return updates
def GetAdUpdate(ad):
    updates = {}
    modifiedtime = ad.get("insertDate") or ad.get("releaseDate")
    id = ad.get("id")
    nowtime  = datetime.now()
    updates = {
        "timestamp":nowtime.timestamp(),
        "lastupdate":getTimeStamp(modifiedtime),
        "lastadId":id,
    }
    return updates

def CreatelastupdateLog(session,typ):
    params = gparams.copy()
    updates = getLastUpdates()
    if typ == "rental":
        catid = "2"
    else:
        catid = "1,3"
    params.update({
        'sorts[0].Name':"_newest",
        "transactionIds":catid,
        'pageSize':1
        })
    d = fetch(session,url,params)
    try:
        data = d['items'][0]
        latupdate = GetAdUpdate(data)
        # print(latupdate)
        updates.update({typ:latupdate})
    except Exception as e:
        traceback.print_exc()
        print("execption ======>" , e)

    # lastupdate = json.load(open(f'{cpath}/lastUpdate.json','r'))
    print(updates)
    with open(f'{cpath}/lastUpdate.json','w') as file:
        file.write(json.dumps(updates))

def asyncUpdateParuvendu():
    params= gparams.copy()
    updates = getLastUpdates()
    # print(updates)
    try:
        session =  HttpRequest(True,'https://ws-web.avendrealouer.fr/',headers,{},{},False,cpath,1,10)
        if not updates:
            CreatelastupdateLog(session,'rental')
            CreatelastupdateLog(session,'sale')
        updates = getLastUpdates()
        for key,val in updates.items():
            CreatelastupdateLog(session,key)
            if key == "rental":
                catid = "2"
            else:
                catid = "1,3"
            params.update({
            'sorts[0].Name':"_newest",
            'transactionIds':catid,
            'pageSize':100,
            })
            updated = False
            p=1
            producer = AsyncKafkaTopicProducer()
            # await producer.statProducer()
            while not updated and p*pagesize<=700:
                print(f"cheking page {p}")
                adsres = parstItems(session,params,page=p,save=False,producer=producer)
                ads = adsres["items"]
                lastad = ads[len(ads)-1]
                webupdates = GetAdUpdate(lastad)
                savedata(adsres,producer=producer)
                res = val['lastupdate']>webupdates['lastupdate']
                print(f"{val['lastupdate']}>{webupdates['lastupdate']} ={res} and type {key}")
                if res:
                    updated = True
                p+=1
                # await producer.stopProducer()
    finally:
        session.__del__()
def rescrapActiveId():
    nowtime = datetime.now()
    nowtime = nowtime - timedelta(hours=1)
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as excuter:
        futures = excuter.map(main,["rental","sale"],[True,True])
        for f in futures:print(f)
    # main("sale",True)
    # main("rental",True)
    saveLastCheck(website,nowtime.isoformat())
    # main("rental",True)
    # main("sale",True)
    # await startCrawling(session,filterParamList,producer=producer)
    # await producer.stopProducer()
def UpdateParuvendu():
    asyncUpdateParuvendu()
if __name__=="__main__":
    url = "https://ws-web.avendrealouer.fr/realestate/properties/"
    asyncio.run(main())

