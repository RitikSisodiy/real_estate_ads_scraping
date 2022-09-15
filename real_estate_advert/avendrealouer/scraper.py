from datetime import datetime
from email import header
from socket import timeout
import traceback
import aiohttp
import asyncio
import os
from requests_html import HTMLSession
import json,re
try:
    from uploader import AsyncKafkaTopicProducer
except:
    from .uploader import AsyncKafkaTopicProducer
kafkaTopicName = "avendrealouer-data_v1"
commonTopicName = "common-ads-data_v1"
s= HTMLSession()
pagesize  = 100 # maxsize is 100
cpath =os.path.dirname(__file__)
url = "https://ws-web.avendrealouer.fr/realestate/properties/"
params = {
        "transactionIdsIds":"1,3", # 2 for location
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
async def fetch(session,url,params = None,method="get",**kwargs):
    # if params:
    #     query_string = urllib.parse.urlencode( params )
    #     url += "?"+query_string 
    try:
        res = await session.get(url,headers = headers,params=params,timeout=10)
    except Exception as e:
        await asyncio.sleep(3)
        return await fetch(session,url,params,method,**kwargs)
    if res.status==200:
        response = await res.json()
    else:
        return await fetch(session,url,params,method,**kwargs)
    return response
def getTimeStamp(strtime):
    formate = '%Y-%m-%d %H:%M:%S'
    # 2022-09-14 20:00:00
    t = datetime.strptime(strtime,formate)
    return t.timestamp()
async def savedata(resjson,**kwargs):
    resstr = ''
    ads = resjson["items"]
    producer = kwargs["producer"]
    await producer.TriggerPushDataList(kafkaTopicName,ads)
    # await producer.TriggerPushDataList(commonTopicName,[ParseLefigaro(ad) for ad in ads])
    # for ad in ads:
    #     resstr += json.dumps(ad)+"\n"
    # with open("output.json",'a') as file:
    #     file.write(resstr)
    # print('saved data')
async def startCrawling(session,param,**kwargs):
    # for param in filterParamList:
        param["size"] = pagesize
        data = await fetch(session,url,param)
        # data= json.load(res)
        await savedata(data,**kwargs)
        totalres = int(data["count"])
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
        for totalpage,param in filterlist:
            # print(param)
            # await asyncio.sleep(5)
            tasks = []
            for i in range(start,totalpage+1):
                param['currentPage'] = i
                # print(param)
                await parstItems(session,param,page=i,**kwargs)
                # tasks.append(asyncio.ensure_future(parstItems(session,param,page=i,**kwargs)))
            await asyncio.gather(*tasks)
            start=1  
        # totaldata = 0
        # for d in data:
        #     results = len(d['feed']["row"])
        #     print(results,"this is len of rows")
        #     totaldata += results
        #     await savedata(d,**kwargs)
        # print(totaldata)
async def getTotalResult(session,params,url):
    totalres = {
        'size':1,
    }
    params.update(totalres)
    r = await fetch(session,url,params=params)
    return int(r.get("count") or 0)
async def getMaxPrize(session,params,url):
    # dic,burl = GetUrlFilterdDict(url)
    # dic["ajaxAffinage"] = 0
    # dic["ddlTri"] = "prix_seul"
    # dic["ddlOrd"] = "desc"
    prizefilter = {
        "sorts[0].Name":"price",
        "size":1
    }
    params.update(prizefilter)
    r = await fetch(session,url,params=params)
    prize = r["items"][0]["price"]
    return float(prize)
async def getFilter(session,params,producer):
    dic,baseurl = params , "https://ws-web.avendrealouer.fr/realestate/properties/"
    # url = getUrl(baseurl,dic)
    # url = baseurl
    maxresult = 700
    totalresult =await getTotalResult(session,dic,baseurl)
    acres = totalresult
    fetchedresult = 0
    iniinterval = [0,1000]
    finalresult = 0
    maxprice = await  getMaxPrize(session,params,baseurl)
    if totalresult>=maxresult:
        while iniinterval[1]<=maxprice:
            print(iniinterval)
            dic['price.gte'],dic['price.lte'] = iniinterval
            totalresult = await getTotalResult(session,dic,baseurl)
            if totalresult <= 1400 and totalresult>0:
                print("condition is stisfy going to next interval")
                print(iniinterval,">apending")
                # filterurllist.append(iniinterval)
                # filterurllist+=json.dumps(dic)+":\n"
                await startCrawling(session,dic,producer=producer)
                # print(filterurllist)
                iniinterval[0] = iniinterval[1]+1
                iniinterval[1] = iniinterval[0]+int(iniinterval[0]/2)
                finalresult +=totalresult
            elif maxresult-totalresult> 200:
                print("elif 1")
                last = 10
                iniinterval[1] = iniinterval[1] + int(iniinterval[1]/last)
            elif totalresult>maxresult:
                print("elif 2")
                last = -50
                iniinterval[1] = iniinterval[1] + int(iniinterval[1]/last)
            print(totalresult,"-",maxresult)
        print(iniinterval,">apending")
        await startCrawling(session,dic,producer=producer)
        finalresult +=totalresult
    # finallsit = [json.loads(d) for d in filterurllist.split(":\n")]
    # print(finallsit)
    # paramslist = []
    # for par in finallsit:
    #     params['filters[P5M0]'],params['filters[P5M1]'] = par
    #     paramslist.append(params)
    print(f"total result is : {acres} filtered result is: {finalresult}")
    # print(filterurllist)
    # time.sleep(10)
    return 0
async def parstItems(session,param,page=None,save=True,**kwargs):
    if page:
        param.update({"from":((page-1)*pagesize)})
    data = await fetch(session,url,param)
    # print(param['p'])
    # data = json.load(res)
    if save:
        await savedata(data,**kwargs)
    return data
async def CheckId(id):
    headers = {
        "user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.63 Safari/537.36"
    }
    async with aiohttp.ClientSession() as session:
        furl  = f"https://ws-web.avendrealouer.fr/realestate/properties/?id={id}"
        # print(furl)
        try:
            await fetch(session,furl,headers=headers)
            return True
        except:return False
async def main(adsType = ""):
    # catid info
    # vente is for  Vente immobilier 
    # location is for Location immobilier
    if adsType == "rental":
        catid = "2"
    else:
        catid = "1,3"
    params["transactionIds"] = catid
    # filterParamList = [*getFilter(param) for param in params
    async with aiohttp.ClientSession() as session:
        await CreatelastupdateLog(session,adsType)
        producer = AsyncKafkaTopicProducer()
        flist = [2,3,6,7,19]
        for f in flist:
            params.update({"typeIds":f})
            await getFilter(session,params,producer)
        # await startCrawling(session,filterParamList,producer=producer)
        await producer.stopProducer()
def main_scraper(payload):
    adtype = payload["real_state_type"]
    if adtype == "Updated/Latest Ads":
        UpdateParuvendu()
    else:
        asyncio.run(main(adtype))
def getLastUpdates():
    try:
        with open(f'{cpath}/lastUpdate.json','r') as file:
            updates = json.load(file)
    except:
        return {}
    return updates
async def GetAdUpdate(ad):
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

async def CreatelastupdateLog(session,typ):
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
    d = await fetch(session,url,params)
    print(d)
    try:
        data = d['items'][0]
        latupdate = await GetAdUpdate(data)
        # print(latupdate)
        updates.update({typ:latupdate})
    except Exception as e:
        traceback.print_exc()
        print("execption ======>" , e)

    # lastupdate = json.load(open(f'{cpath}/lastUpdate.json','r'))
    print(updates)
    with open(f'{cpath}/lastUpdate.json','w') as file:
        file.write(json.dumps(updates))

async def asyncUpdateParuvendu():
    updates = getLastUpdates()
    # print(updates)
    async with aiohttp.ClientSession() as session:
        if not updates:
            await CreatelastupdateLog(session,'rental')
            await CreatelastupdateLog(session,'sale')
        updates = getLastUpdates()
        for key,val in updates.items():
            await CreatelastupdateLog(session,key)
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
            await producer.statProducer()
            while not updated and p*pagesize<=700:
                print(f"cheking page {p}")
                adsres = await parstItems(session,params,page=p,save=False,producer=producer)
                ads = adsres["items"]
                lastad = ads[len(ads)-1]
                webupdates = await GetAdUpdate(lastad)
                await savedata(adsres,producer=producer)
                res = val['lastupdate']>webupdates['lastupdate']
                print(f"{val['lastupdate']}>{webupdates['lastupdate']} ={res} and type {key}")
                if res:
                    updated = True
                p+=1
            await producer.stopProducer()

def UpdateParuvendu():
    asyncio.run(asyncUpdateParuvendu())
if __name__=="__main__":
    url = "https://ws-web.avendrealouer.fr/realestate/properties/"
    asyncio.run(main())

