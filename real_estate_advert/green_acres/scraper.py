from datetime import datetime
import traceback
import aiohttp
import asyncio
import os
from requests_html import HTMLSession, HTML
import json, re
from .parser import ParseGreenAcre
from HttpRequest.uploader import AsyncKafkaTopicProducer

kafkaTopicName = "green-acres-data_v1"
commonTopicName = "common-ads-data_v1"

s = HTMLSession()
pagesize = 100  # maxsize is 100
cpath = os.path.dirname(__file__) or "."
url = "https://apifront.green-acres.com/api/v8.0/visitors/registered/listings/search"
params = json.load(open(cpath + "/filter.json"))
headers = {
    "Accept": "application/problem+json",
    "Content-Type": "application/json",
    "User-Agent": "Green-Acres / 1.9.3 (242910); (Google Android SDK built for x86; SDK 27; Android 8.1.0); prod",
    "Authorization": "ea4ef51d-fe36-4fc0-911a-8ea95519a4d2",
    "Accept-Language": "en",
    "Host": "apifront.green-acres.com",
    "Connection": "Keep-Alive",
    "Accept-Encoding": "gzip",
}


async def fetch(session, url, method="get", jsonresponce=True, **kwargs):
    header = headers
    if kwargs.get("headers"):
        header = kwargs["headers"]
        del kwargs["headers"]
    try:
        if method == "get":
            res = await session.get(url, headers=header, **kwargs)
        if method == "post":
            res = await session.post(url, headers=header, **kwargs)
    except Exception as e:
        await asyncio.sleep(3)
        return await fetch(session, url, method, jsonresponce, **kwargs)
    if jsonresponce:
        response = await res.json()
        return response
    else:
        content = await res.text()
        html = HTML(html=content)
        return html


def getTimeStamp(strtime):
    formate = "%Y-%m-%dT%H:%M:%S"
    t = datetime.strptime(strtime, formate)
    return int(t.timestamp())


async def getDeription(session, id):
    url = f"https://www.green-acres.fr/fr/properties/{id}.htm"
    r = await fetch(
        session,
        url,
        jsonresponce=False,
        headers={
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36"
        },
    )
    return des


async def savedata(session, ads, **kwargs):
    print(len(ads))
    producer = kwargs["producer"]
    dif = 10
    for i in range(0, len(ads), dif):
        a = i // dif
        tads = []
        for ad in ads[a * dif : (a * dif) + dif]:
            if len(ad["summary"]) == 0:
                ad["summary"] = await getDeription(session, ad["id"])
            tads.append(ad)
        await producer.TriggerPushDataList(kafkaTopicName, tads)
        ads = [ParseGreenAcre(ad) for ad in tads]
        await producer.TriggerPushDataList(commonTopicName, ads)


async def startCrawling(session, param, **kwargs):
    param["searchConfiguration"]["pageSize"] = 1
    data = await fetch(session, url, method="post", json=param)
    await savedata(session, data["result"]["adverts"], **kwargs)
    totalres = int(data["result"]["totalAdverts"])
    totalpage = (totalres // 19) + 1
    param["searchConfiguration"]["pageSize"] = totalpage
    totalpage = int(totalpage) if totalpage == int(totalpage) else int(totalpage) + 1
    print(totalres, param)
    tasks = []
    print(totalpage, "this is total pages")
    for i in range(1, totalpage + 1):
        param["searchConfiguration"].update({"pageNumber": i})
        await parstItems(session, param, page=i, **kwargs)
    await asyncio.gather(*tasks)


async def getTotalResult(session, params, url):
    totalres = {
        "pageSize": 1,
    }
    params.update(totalres)
    print(params)
    r = await fetch(session, url, method="post", json=params)
    return int(r["result"]["totalAdverts"])


async def getMaxPrize(session, params, url):
    prizefilter = {"sort": 7, "pageSize": 1}
    params.update(prizefilter)
    r = await fetch(session, url, params=params)
    prize = r["classifieds"][0]["priceLabel"]
    prize = float(re.search("[0-9.]+", prize).group())
    return prize


async def parstItems(session, param, page=None, save=True, **kwargs):
    if page:
        param.update({"pageNumber": page})
    data = await fetch(session, url, method="post", json=param)
    if save:
        await savedata(session, data["result"]["adverts"], **kwargs)
        return None
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


async def main(adsType=""):
    async with aiohttp.ClientSession() as session:
        await CreatelastupdateLog(session, adsType)
        producer = AsyncKafkaTopicProducer()
        await startCrawling(session, params, producer=producer)
        await producer.stopProducer()


def main_scraper(payload):
    adtype = payload["real_state_type"]
    if adtype == "Updated/Latest Ads":
        UpdateGreenacres()
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
    modifiedtime = ad.get("lastAdvertUpdate") or ad.get("creationDate")
    id = ad.get("id")
    nowtime = datetime.now()
    updates = {
        "timestamp": nowtime.timestamp(),
        "lastupdate": getTimeStamp(modifiedtime),
        "lastadId": id,
    }
    return updates


async def CreatelastupdateLog(session, typ):
    updates = getLastUpdates()

    params["searchConfiguration"].update({"sortOrder": "data_d", "pageSize": 1})
    print(params)
    d = await fetch(session, url, method="post", json=params)
    try:
        data = d["result"]["adverts"][0]
        latupdate = await GetAdUpdate(data)
        updates.update({typ: latupdate})
    except Exception as e:
        traceback.print_exc()
        print("execption ======>", e)

    print(updates)
    with open(f"{cpath}/lastUpdate.json", "w") as file:
        file.write(json.dumps(updates))


async def asyncUpdateGreenacres():
    updates = getLastUpdates()
    async with aiohttp.ClientSession() as session:
        if not updates:
            await CreatelastupdateLog(session, "sale")
        updates = getLastUpdates()
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
                await savedata(session, adsres, producer=producer)
                res = val["lastupdate"] > webupdates["lastupdate"]
                print(
                    f"{val['lastupdate']}>{webupdates['lastupdate']} ={res} and type {key}"
                )
                if res:
                    updated = True
                p += 1
            await producer.stopProducer()


def UpdateGreenacres():
    asyncio.run(asyncUpdateGreenacres())


if __name__ == "__main__":
    url = "https://fi-classified-search-api.immo.fcms.io/apps/classifieds"
    asyncio.run(main())
