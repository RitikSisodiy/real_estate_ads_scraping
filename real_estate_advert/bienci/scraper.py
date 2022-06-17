import asyncio
import json
import os
from requests_html import AsyncHTMLSession
import random
try:
    from fetch import fetch
    from uploader import AsyncKafkaTopicProducer
except:
    from .fetch import fetch
    from .uploader import AsyncKafkaTopicProducer

kafkaTopicName = "bienici_data_v1"
# define your filter here
cpath =os.path.dirname(__file__) 
citys = open(f"{cpath}/finalcitys.json",'r').readlines()
citys = [[json.loads(d)['name'],json.loads(d)['zoneIds'] ] for d in citys]
citys.sort()
lastpin ,lastpage = None,1
if lastpin:start = False
else:start = True
def getFilterUrl(Filter):
    Filter = json.dumps(Filter)
    url  = f"https://www.bienici.com/realEstateAds.json?filters={Filter}&extensionType=extendedIfNoResult"
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
asyncpage = 10
# change the url of filter data to scrap all the ads
async def main():
    tasks = []
    count = 0
    global start
    global lastpin
    global lastpage
    session = AsyncHTMLSession()
    producer = AsyncKafkaTopicProducer()
    await producer.statProducer()
    # session.proxies.update({"http": "socks5://218.1.142.41:57114", "https": "socks5://218.1.142.41:57114"})
    print(lastpin, "this ", start)
    Filter = {
    "size":500,
    "from":0,
    "showAllModels":False,
    "filterType":"buy",
    "propertyType":["house","flat"],
    "newProperty":False,
    "page":3,
    "sortBy":"relevance",
    "sortOrder":"desc",
    "onTheMarket":[True],
    "zoneIdsByTypes":{"zoneIds":["-126506","-126513","-126516","-126525","-126523","-126514","-126519","-126512","-126528","-121747","-126515","-126520","-126508","-126529"]}
    }
    for pin,zonid in citys:
        if lastpin == pin:
            print("this is last pin")
            start = True 
        if start:
            count +=1
            Filter['zoneIdsByTypes']['zoneIds'] = zonid
            baseurl = getFilterUrl(Filter)
            tasks.append(asyncio.ensure_future(GetAllPages(baseurl,session,first=True,Filter=Filter,producer=producer)))
            if len(tasks) == asyncpage or count == len(citys):
                await asyncio.gather(*tasks)
                t=random.randint(5,15)
                print(f"wait for {t} seconds")
                await asyncio.sleep(t)
                tasks=[]
    await asyncio.gather(*tasks)
    await producer.stopProducer()

async def saveRealstateAds(ads,**kwargs):
    producer = kwargs.get("producer")
    await producer.TriggerPushDataList(kafkaTopicName,ads)
    # allads = ''
    # for ad in ads:
    #     allads+= json.dumps(ad)+"\n"
    # with open("output/output.json",'a') as file:
    #     file.write(allads)

async def GetAllPages(baseurl,session,first=False,Filter=None,**kwargs):
    r= await fetch(baseurl,session,Json=True)
    if r:
        print(baseurl,"-200")
        await saveRealstateAds(r['realEstateAds'],**kwargs)
        if first:
            totalpage = r['total']/Filter['size']
            totalpage = int(totalpage)+1 if totalpage>int(totalpage) else int(totalpage)
            print(len(r["realEstateAds"]),r['total'],"tis is ")
            tasks =[]
            for i in range(2,totalpage):
                Filter['from'] += Filter['size'] 
                url = getFilterUrl(Filter)
                tasks.append(asyncio.ensure_future(GetAllPages(baseurl,session,first=True,Filter=Filter,**kwargs)))
            await asyncio.gather(*tasks)


def main_scraper():
    asyncio.run(main())
if __name__ == "__main__":
    asyncio.run(main())

