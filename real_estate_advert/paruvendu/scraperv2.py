from datetime import datetime
import traceback
import aiohttp
import asyncio
import os
from requests_html import HTML
import json
from .parser import ParseParuvendu
try:
    from getfiterparam import getFilter
except:
    from .getfiterparam import getFilter
try:
    from uploader import AsyncKafkaTopicProducer
except:
    from .uploader import AsyncKafkaTopicProducer
pagesize  = 100 # maxsize is 100
cpath =os.path.dirname(__file__)
url = "https://www.paruvendu.fr/communfo/appmobile/default/pa_search_list"
params = {
        'ver':'4.1.4',
        'itemsPerPage':'12',
        'mobId':'dFlRt5PY0Gg:APA91bHEo1pJb5ChJsqkD-kzsxabz7I3tE8UctG_yN9_Do7_3QQM5ecUvw9jJln3Tm4UxghOmk4H2jozt9dZ8QFu2KuDWwc16av2QQ3SZOVCInP6TB5af9xoW2m_tvpc885HY4JZqsmd',
        'p':'1',
        'sortOn':'dateMiseEnLigne',
        'sortTo':'DESC',
        'key':'lafNgtmagb6VrZugp7Wim2SUf32gZtask2iomq+lo891jsyq2N2Sx9+dfZnaqpm3lqZ+l6qDn8F1opyRmIODsWbGxIXb3GrPr9KimbrSlbermZ+HnqCaqmOd1KzZa5a+abKnpZewpqm83ZeXnMbXlISDaouLhLyleqG1aLl0Z8WXmtmfvJeexNncpMmenZaqjGaBkqKo08ZUVpZommZkmmSTmmCIiKPKy6eY2KaZwpSohW6tqG2pypKh',
    }
async def fetch(session,url,params = None,method="get",**kwargs):
    # if params:
    #     query_string = urllib.parse.urlencode( params )
    #     url += "?"+query_string 
    try:
        res = await session.get(url,params=params)
    except Exception as e:
        await asyncio.sleep(3)
        return await fetch(session,url,params,method,**kwargs)
    response = await res.json()
    return response

async def savedata(resjson,**kwargs):
    resstr = ''
    ads = resjson['feed']["row"]
    producer = kwargs["producer"]
    await producer.TriggerPushDataList('paruvendu-data_v1',ads)
    await producer.TriggerPushDataList('common-ads-data_v1',[ParseParuvendu[ad] for ad in ads])
    # for ad in ads:
    #     resstr += json.dumps(ad)+"\n"
    # with open("output.json",'a') as file:
    #     file.write(resstr)
    # print('saved data')s
async def startCrawling(session,filterParamList,**kwargs):
    for param in filterParamList:
        param['showdetail'] = 1
        param["itemsPerPage"] = pagesize
        data = await fetch(session,url,param)
        # data= json.load(res)
        await savedata(data,**kwargs)
        totalres = int(data["feed"]["@totalResults"])
        totalpage = totalres/pagesize
        totalpage = int(totalpage) if totalpage==int(totalpage) else int(totalpage)+1
        print(totalres,param)
        tasks = []
        print(totalpage,"this is total pages")
        for i in range(2,totalpage+1):
            param['p'] = i
            # print(param)
            tasks.append(asyncio.ensure_future(parstItems(session,param,page=i,**kwargs)))
        data = await asyncio.gather(*tasks)  
        totaldata = 0
        for d in data:
            results = len(d['feed']["row"])
            print(results,"this is len of rows")
            totaldata += results
            await savedata(d,**kwargs)
        print(totaldata)
async def parstItems(session,param,page=None,**kwargs):
    param.update({"p":page})
    data = await fetch(session,url,param)
    # print(param['p'])
    # data = json.load(res)
    return data
    await savedata(data,**kwargs)
async def main(adsType = ""):
    # catid info
    # IVH00000 is for  Vente immobilier 
    # ILH00000 is for Location immobilier
    if adsType == "sale":
        catid = "IVH00000"
    else:
        catid = "ILH00000"
    params["catId"] = catid
    params["filters[_R1]"] = catid
    # filterParamList = [*getFilter(param) for param in params
    async with aiohttp.ClientSession() as session:
        await CreatelastupdateLog(session,adsType)
        filterParamList = getFilter(params)
        producer = AsyncKafkaTopicProducer()
        await startCrawling(session,filterParamList,producer=producer)
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
async def GetAdUpdate(session,adurl):
    updates = {}
    headers = {
        "user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.63 Safari/537.36"
    }
    res = await session.get(adurl,headers=headers)
    doc = await res.text()
    soup = HTML(html=doc)
    print(adurl)
    modifiedtime = soup.find("meta[property='og:url']",first=True).attrs.get("content").split("=")[1]
    id = soup.find("div[data-id]",first=True).attrs['data-id'] 
    nowtime  = datetime.now()
    updates = {
        "timestamp":nowtime.timestamp(),
        "lastupdate":modifiedtime,
        "lastadId":id,
        "source":adurl
    }
    return updates

async def CreatelastupdateLog(session,typ):
    updates = getLastUpdates()
    if typ == "sale":
        catid = "IVH00000"
    else:
        catid = "ILH00000"
    params.update({
        'sortOn':'dateMiseEnLigne',
        'sortTo':'DESC',
        'catId':catid,
        "filters[_R1]":catid,
        'itemsPerPage':1
        })
    d = await fetch(session,url,params)
    try:
        data = d['feed']['row'][0]
        adurl = data['shortURL']
        latupdate = await GetAdUpdate(session,adurl=adurl)
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
            if key == "sale":
                catid = "IVH00000"
            else:
                catid = "ILH00000"
            params.update({
            'sortOn':'dateMiseEnLigne',
            'sortTo':'DESC',
            'catId':catid,
            'itemsPerPage':100,
            'showdetail':1
            })
            updated = False
            p=1
            producer = AsyncKafkaTopicProducer()
            while not updated:
                print(f"cheking page {p}")
                adsres = await parstItems(session,params,page=p)
                ads = adsres['feed']['row']
                lastadurl = ads[len(ads)-1]['shortURL']
                webupdates = await GetAdUpdate(session,lastadurl)
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
    url = "https://www.paruvendu.fr/communfo/appmobile/default/pa_search_list"
    asyncio.run(main())

