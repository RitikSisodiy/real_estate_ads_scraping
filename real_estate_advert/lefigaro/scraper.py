from datetime import datetime, timedelta
import traceback, concurrent.futures
import aiohttp
import asyncio
import os, settings
from requests_html import HTMLSession
import json, re

from saveLastChaeck import saveLastCheck
from .parser import ParseLefigaro
from HttpRequest.uploader import AsyncKafkaTopicProducer

kafkaTopicName = settings.KAFKA_LEFIGARO
commonTopicName = settings.KAFKA_COMMON_PATTERN
website = "immobilier.lefigaro.fr"
commonIdUpdate = f"activeid-{website}"
s = HTMLSession()
pagesize = 1000
cpath = os.path.dirname(__file__) or "."
url = "https://fi-classified-search-api.immo.fcms.io/apps/classifieds"
params = {
    "version": 1,
    "currentPage": 1,
    "pageSize": 1000,
    "transaction": "vente",
    "sort": 5,
}
headers = {
    "Accept": "*/*",
    "x-api-key": "AIzaSyDb4PeV9gi5UY_Z3-27ygjOm8PV950j9Us",
    "Referer": "http://figaroImmo-Android/",
    "source": "FigaroImmo appli mobile",
    "User-Agent": "FigaroImmo  (sdk_gphone_x86; Android 11; en_US)",
    "device-token": "cK4ASmJ1T4KVS8gzo0six1:APA91bHp76JTgTJKAyr5M_NWuuNUvS9Lrl6Q10GYaNvJna7gmNvhcCaqrfvBLd0HzIMlZyZaK0pEknEBzGq8queVKHHi4i6btx9kUZfJSh_5ss1Xxax6b66L0KCxsBN9z5jD7_Caeosf",
    "Host": "fi-classified-search-api.immo.fcms.io",
    "Connection": "Keep-Alive",
    "Accept-Encoding": "gzip",
}


async def fetch(session, url, params=None, method="get", **kwargs):
    """
    Fetches the data from the specified URL.

    Args:
        session (aiohttp.ClientSession): The session object to use to make the request.
        url (str): The URL of the resource to fetch.
        params (dict): The parameters to pass to the request.
        method (str): The HTTP method to use.
        **kwargs: Keyword arguments that will be passed to the requests library.

    Returns:
        The response data.

    Raises:
        Exception: If an error occurs.
    """
    try:
        res = await session.get(url, headers=headers, params=params)
    except Exception as e:
        await asyncio.sleep(3)
        return await fetch(session, url, params, method, **kwargs)
    response = await res.json()
    return response


def getTimeStamp(strtime):
    formate = "%Y-%m-%dT%H:%M:%S"
    t = datetime.strptime(strtime, formate)
    return t.timestamp()


async def savedata(resjson, **kwargs):
    # save the scraped data
    ads = resjson["classifieds"]
    producer = kwargs["producer"]
    if kwargs.get("onlyid"):
        ads = [ParseLefigaro(ad) for ad in ads]
        await producer.TriggerPushDataList_v1(commonIdUpdate, ads)
    else:
        await producer.TriggerPushDataList(kafkaTopicName, ads)
        ads = [ParseLefigaro(ad) for ad in ads]
        await producer.TriggerPushDataList("common-ads-data_v1", ads)


async def startCrawling(session, param, **kwargs):
    """
    Start crawling the specified URL.

    Args:
        session (aiohttp.ClientSession): The session object to use to make the request.
        param (dict): The parameters to pass to the request.
        **kwargs: Keyword arguments that will be passed to the `savedata()` function.

    Returns:
        None.
    """
    param["showdetail"] = 1
    param["pageSize"] = pagesize
    data = await fetch(session, url, param)
    await savedata(data, **kwargs)
    totalres = int(data["total"])
    totalpage = totalres / pagesize
    totalpage = int(totalpage) if totalpage == int(totalpage) else int(totalpage) + 1
    print(totalres, param)
    tasks = []
    print(totalpage, "this is total pages")
    for i in range(2, totalpage + 1):
        param["currentPage"] = i
        await parstItems(session, param, page=i, **kwargs)
    await asyncio.gather(*tasks)


async def getTotalResult(session, params, url):
    totalres = {"itemsPerPage": 1, "showdetail": 0}
    params.update(totalres)
    r = await fetch(session, url, params=params)
    return int(r["total"])


async def getMaxPrize(session, params, url):
    prizefilter = {"sort": 7, "pageSize": 1}
    params.update(prizefilter)
    r = await fetch(session, url, params=params)
    prize = r["classifieds"][0]["priceLabel"]
    prize = float(re.search("[0-9.]+", prize).group())
    return prize


async def getFilter(session, params, producer, onlyid=False):
    """
    Get the listings that match the specified filter criteria.

    Args:
        session (aiohttp.ClientSession): The session object to use to make the request.
        params (dict): The parameters to pass to the request.
        producer (asyncio.Queue): The queue to use to send the listings to.
        onlyid (bool): Whether to only get the listing IDs or the full listings.

    Returns:
        None.
    """
    dic, baseurl = (
        params,
        "https://fi-classified-search-api.immo.fcms.io/apps/classifieds",
    )
    maxresult = 10000
    totalresult = await getTotalResult(session, dic, baseurl)
    acres = totalresult
    fetchedresult = 0
    iniinterval = [0, 1000]
    finalresult = 0
    maxprice = await getMaxPrize(session, params, baseurl)
    if totalresult >= maxresult:
        while iniinterval[1] <= maxprice:
            dic["priceMin"], dic["priceMax"] = iniinterval
            totalresult = await getTotalResult(session, dic, baseurl)
            if totalresult < maxresult and maxresult - totalresult <= 2000:
                print("condition is stisfy going to next interval")
                print(iniinterval, ">apending")
                await startCrawling(session, dic, producer=producer, onlyid=onlyid)
                iniinterval[0] = iniinterval[1] + 1
                iniinterval[1] = iniinterval[0] + int(iniinterval[0] / 2)
                finalresult += totalresult
            elif maxresult - totalresult > 200:
                print("elif 1")
                last = 10
                iniinterval[1] = iniinterval[1] + int(iniinterval[1] / last)
            elif totalresult > maxresult:
                print("elif 2")
                last = -10
                iniinterval[1] = iniinterval[1] + int(iniinterval[1] / last)
            print(totalresult, "-", maxresult)
        print(iniinterval, ">apending")
        await startCrawling(session, dic, producer=producer)
        finalresult += totalresult
    print(f"total result is : {acres} filtered result is: {finalresult}")
    return 0


async def parstItems(session, param, page=None, save=True, **kwargs):
    if page:
        param.update({"currentPage": page})
    data = await fetch(session, url, param)
    if save:
        await savedata(data, **kwargs)
    return data


async def CheckId(id):
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.63 Safari/537.36"
    }
    async with aiohttp.ClientSession() as session:
        furl = f"https://fi-classified-search-api.immo.fcms.io/apps/classifieds?paId={id}&showdetail=1&mobId=dFlRt5PY0Gg:APA91bHEo1pJb5ChJsqkD-kzsxabz7I3tE8UctG_yN9_Do7_3QQM5ecUvw9jJln3Tm4UxghOmk4H2jozt9dZ8QFu2KuDWwc16av2QQ3SZOVCInP6TB5af9xoW2m_tvpc885HY4JZqsmd&key=lafNgtmagb6VrZugp7Wim2SUf32gZtask2iomq+lo891jsyq2N2Sx9+dfZnaqpm3lqZ+l6qDn8F1opyRmIODsWbGxIXb3GrPr9KimbrSlbermZ+HnqCaqmOd1KzZa5a+abKnpZewpqm83ZeXnMbXlISDaouLhLyleqG1aLl0Z8WXmtmfvJeexNncpMmenZaqjGaBkqKo08ZUVpZommZkmmSTmmCIiKPKy6eY2KaZwpSohW6tqG2pypKh"
        r = await fetch(session, furl, headers=headers)
        data = r
        print(len(data["feed"]["row"]))
        if data and len(data["feed"]["row"]) == 1:
            return True
        else:
            return False


async def main(adsType="", onlyid=False):
    param = params.copy()
    if adsType == "rental":
        catid = "vente"
    else:
        catid = "location"
    param["transaction"] = catid
    async with aiohttp.ClientSession() as session:
        if not onlyid:
            await CreatelastupdateLog(session, adsType)
        producer = AsyncKafkaTopicProducer()
        filterParamList = await getFilter(session, param, producer, onlyid=onlyid)
        await producer.stopProducer()


def main_scraper(payload):
    adtype = payload["real_state_type"]
    if adtype == "Updated/Latest Ads":
        UpdateParuvendu()
    elif payload.get("real_state_type") == "deletedCheck":
        rescrapActiveId()
    else:
        asyncio.run(main(adtype))


def getLastUpdates():
    try:
        with open(f"{cpath}/lastUpdate.json", "r") as file:
            updates = json.load(file)
    except:
        return {}
    return updates


async def GetAdUpdate(ad):
    updates = {}
    modifiedtime = ad.get("updatedAt") or ad.get("creationDate")
    id = ad.get("id")
    nowtime = datetime.now()
    updates = {
        "timestamp": nowtime.timestamp(),
        "lastupdate": getTimeStamp(modifiedtime),
        "lastadId": id,
        "source": ad.get("recordLink"),
    }
    return updates


async def CreatelastupdateLog(session, typ):
    updates = getLastUpdates()
    if typ == "rental":
        catid = "vente"
    else:
        catid = "location"
    params.update({"sort": 5, "transaction": catid, "pageSize": 1})
    d = await fetch(session, url, params)
    print(d)
    try:
        data = d["classifieds"][0]
        latupdate = await GetAdUpdate(data)
        updates.update({typ: latupdate})
    except Exception as e:
        traceback.print_exc()
        print("execption ======>", e)

    print(updates)
    with open(f"{cpath}/lastUpdate.json", "w") as file:
        file.write(json.dumps(updates))


async def asyncUpdateParuvendu():
    updates = getLastUpdates()
    async with aiohttp.ClientSession() as session:
        if not updates:
            await CreatelastupdateLog(session, "rental")
            await CreatelastupdateLog(session, "sale")
            updates = {}
        for key, val in updates.items():
            await CreatelastupdateLog(session, key)
            if key == "rental":
                catid = "vente"
            else:
                catid = "location"
            params.update(
                {"sort": 5, "transaction": catid, "pageSize": 100, "showdetail": 1}
            )
            updated = False
            p = 1
            producer = AsyncKafkaTopicProducer()
            while not updated and p <= 100:
                print(f"cheking page {p}")
                adsres = await parstItems(
                    session, params, page=p, save=False, producer=producer
                )
                ads = adsres["classifieds"]
                lastad = ads[len(ads) - 1]
                webupdates = await GetAdUpdate(lastad)
                await savedata(adsres, producer=producer)
                res = val["lastupdate"] > webupdates["lastupdate"]
                print(
                    f"{val['lastupdate']}>{webupdates['lastupdate']} ={res} and type {key}"
                )
                if res:
                    updated = True
                p += 1
            await producer.stopProducer()


def rescrapActiveIdbyType(typ):
    asyncio.run(main(typ, True))


def rescrapActiveId():
    nowtime = datetime.now()
    nowtime = nowtime - timedelta(hours=1)
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as excuter:
        futures = [excuter.submit(rescrapActiveIdbyType, i) for i in ["rental", "sale"]]
        for f in futures:
            print(f)
    saveLastCheck(website, nowtime.isoformat())


def UpdateParuvendu():
    asyncio.run(asyncUpdateParuvendu())


if __name__ == "__main__":
    url = "https://fi-classified-search-api.immo.fcms.io/apps/classifieds"
    asyncio.run(main())
