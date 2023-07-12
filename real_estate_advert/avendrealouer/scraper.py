from datetime import datetime, timedelta
from saveLastChaeck import saveLastCheck
import traceback, concurrent.futures
import asyncio, requests
import os
from requests_html import HTMLSession
import json, re, time
from HttpRequest.uploader import AsyncKafkaTopicProducer
from HttpRequest.requestsModules import HttpRequest
import settings
from .parser import ParseAvendrealouer

website = "avendrealouer.fr"
kafkaTopicName = settings.KAFKA_AVENDREALOUER
commonTopicName = settings.KAFKA_COMMON_PATTERN
commonIdUpdate = f"activeid-{website}"
s = HTMLSession()
pagesize = 100  # maxsize is 100
cpath = os.path.dirname(__file__) or "."
url = "https://ws-web.avendrealouer.fr/realestate/properties/"
ajencyurl = "https://ws-web.avendrealouer.fr/common/accounts/?id="
gparams = {
    "typeIds": "2,3,6,7,19",
    "size": 25,
    "transactionIds": "vente",
    "from": 0,
    "sorts[0].Name": "_newest",
    "sorts[0].Order": "desc",
}
headers = {
    "Accept": "*/*",
    "Authorization": "Basic ZWQ5NjUwYTM6Y2MwZDE4NTRmZmE5MzYyODE2NjQ1MmQyMjU4ZWMxNjI=",
    "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 11; sdk_gphone_x86 Build/RSR1.201013.001)",
    "Host": "ws-web.avendrealouer.fr",
    "Connection": "Keep-Alive",
    "Accept-Encoding": "gzip",
}


def fetch(session, url, params=None, method="get", retry=0, **kwargs):
    """
    Fetches data from a given URL using the provided session object and parameters.

    Args:
        session: The session object used for making the HTTP request.
        url: The URL to fetch the data from.
        params: Optional dictionary of query parameters to include in the request.
        method: The HTTP method to use (default is "get").
        retry: The number of times to retry the request in case of failure (default is 0).
        **kwargs: Additional keyword arguments to pass to the fetch function.

    Returns:
        The response data as a JSON object if the request is successful, or None if all retries failed.
    """
    if retry > 3:
        return None
    retry += 1
    try:
        res = session.fetch(url, headers=headers, params=params)
    except Exception as e:
        # In case of an exception, wait for 3 seconds and retry the request
        time.sleep(3)
        return fetch(session, url, params, method, retry=retry, **kwargs)
    if res and res.status_code == 200:
        response = res.json()
    else:
        # Retry the request if the response status code is not 200
        return fetch(session, url, params, method, retry=retry, **kwargs)
    return response


def getTimeStamp(strtime):
    formate = "%Y-%m-%d %H:%M:%S"
    t = datetime.strptime(strtime, formate)
    return t.timestamp()


def savedata(resjson, **kwargs):
    """
    Saves data from a JSON response, using a producer object, and optional keyword arguments.

    Args:
        resjson (dict): A dictionary containing the JSON response.
        **kwargs: Optional keyword arguments.

    Returns:
        None
    """
    ads = resjson["items"]
    producer = kwargs["producer"]
    if kwargs.get("onlyid"):
        ads = [ParseAvendrealouer(ad) for ad in ads]
        producer.PushDataList_v1(commonIdUpdate, ads)
    else:
        producer.PushDataList(kafkaTopicName, ads)
        # Parse ads and push data to commonTopicName using PushDataList method
        ads = [ParseAvendrealouer(ad) for ad in ads]
        producer.PushDataList(commonTopicName, ads)
        print("saved")


def startCrawling(session, param, **kwargs):
    """
    Starts the crawling process using the provided session, parameters, and optional keyword arguments.

    Args:
        session: The session object used for making the HTTP requests.
        param (dict): A dictionary of parameters for the crawling process.
        **kwargs: Optional keyword arguments.

    Returns:
        None
    """
    param["size"] = pagesize
    data = fetch(session, url, param)
    savedata(data, **kwargs)
    totalres = data.get("count") or 0
    totalpage = totalres / pagesize
    totalpage = int(totalpage) if totalpage == int(totalpage) else int(totalpage) + 1
    print(totalres, param)
    print(totalpage, "this is total pages")
    filterlist = []
    if totalpage > 7:
        param.update({"sorts[0].Order": "desc"})
        filterlist.append((7, param))
        param.update({"sorts[0].Order": "asc"})
        filterlist.append(((totalpage - 7), param))
    else:
        filterlist.append((totalpage, param))
    start = 2
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as excuter:
        futures = []
        for totalpage, param in filterlist:
            for i in range(start, totalpage + 1):
                param.update({"currentPage": i})
                parmacopy = param.copy()
                futures.append(
                    excuter.submit(parstItems, session, parmacopy, page=i, **kwargs)
                )
            start = 1
        for f in futures:
            print(f)


def getTotalResult(session, params, url):
    totalres = {
        "size": 1,
    }
    params.update(totalres)
    r = fetch(session, url, params=params)
    return int(r.get("count") or 0)


def getMax(session, params, url, max="price"):
    prizefilter = {"sorts[0].Name": max, "size": 1, "from": 0, "sorts[0].Order": "desc"}
    params.update(prizefilter)
    r = fetch(session, url, params=params)
    print(params)
    try:
        prize = r["items"][0][max]
    except:
        prize = 0
    return float(prize)


def getFilter(
    session, params, producer, onlyid=False, low="price.gte", max="price.lte"
):
    """
    Retrieves filtered URLs and initiates the crawling process for each filter.

    Args:
        session: The session object used for making the HTTP requests.
        params (dict): A dictionary of parameters for filtering.
        producer: The producer object used for data storage.
        onlyid (bool): Indicates if only the IDs should be retrieved (default is False).
        low (str): The lower filter parameter name (default is "price.gte").
        max (str): The upper filter parameter name (default is "price.lte").

    Returns:
        list: A list of filtered URLs.
    """
    dic, baseurl = (
        params.copy(),
        "https://ws-web.avendrealouer.fr/realestate/properties/",
    )
    maxresult = 1400
    try:
        if low is not "price.gte":
            dic[low] = 0
    except:
        pass
    print("fidc", dic)
    totalresult = getTotalResult(session, dic, baseurl)
    acres = totalresult
    fetchedresult = 0
    if low == "price.gte":
        iniinterval = [0, 1000]
        maxprice = getMax(session, params, baseurl)
    else:
        maxprice = getMax(session, params, baseurl, max="surface")
        iniinterval = [0, 1]
    finalresult = 0
    filterurllist = []

    while iniinterval[1] <= maxprice:
        dic[low], dic[max] = iniinterval
        totalresult = getTotalResult(session, dic, baseurl)
        if totalresult <= 1400 and totalresult > 0:
            filterurllist.append(dic.copy())
            startCrawling(session, dic, producer=producer, onlyid=onlyid)
            iniinterval[0] = iniinterval[1] + 1
            iniinterval[1] = iniinterval[0] + int(iniinterval[0] / 2)
            finalresult += totalresult
        elif totalresult == 0:
            last = 10
            iniinterval[0] = iniinterval[1]
            iniinterval[1] = iniinterval[0] + (int(iniinterval[1] / last) or 1)
        elif (
            iniinterval[1] - iniinterval[0] <= 2
            and totalresult > maxresult
            and low == "price.gte"
        ):
            finalresult += totalresult
            getFilter(
                session, dic.copy(), producer, onlyid, "surface.gte", "surface.lte"
            )
            iniinterval[0] = iniinterval[1]
            iniinterval[1] += 1
        elif totalresult > maxresult:
            last = -5
            dif = iniinterval[1] - iniinterval[0]
            iniinterval[1] = iniinterval[1] + int(dif / -2)
            if iniinterval[0] > iniinterval[1]:
                iniinterval[1] = iniinterval[0] + 1
        print(
            f"\r{totalresult}-{maxresult}::::{acres} of  {finalresult}==>{iniinterval} {maxprice} {low} no of filter",
            end="",
        )
    filterurllist.append(dic.copy())
    print(filterurllist)
    return filterurllist


def parstItems(session, param, page=None, save=True, **kwargs):
    if page:
        param.update({"from": ((page - 1) * pagesize)})
    data = fetch(session, url, param)
    if save:
        savedata(data, **kwargs)
    return data


def CheckId(id):
    furl = f"https://ws-web.avendrealouer.fr/realestate/properties/?id={id}"
    try:
        r = requests.get(furl, headers=headers)
        if r.status_code == 200:
            return True
        else:
            return False
    except:
        return False


def main(adsType="", onlyid=False):
    """
    Main function to process ads data based on the provided ads type and onlyid flag.

    Args:
        adsType (str): Type of ads. Default is an empty string.
        onlyid (bool): Flag indicating whether to retrieve only ad IDs. Default is False.

    Returns:
        None
    """
    params = gparams.copy()
    try:
        # Create an instance of HttpRequest
        session = HttpRequest(
            True,
            "https://ws-web.avendrealouer.fr/",
            headers,
            {},
            {},
            False,
            cpath,
            1,
            10,
        )
        # Determine the category ID based on the adsType
        if adsType == "rental":
            catid = "2"
        else:
            catid = "1,3"

        # Update the parameters with the transaction IDs
        params["transactionIds"] = catid
        CreatelastupdateLog(session, adsType)
        producer = AsyncKafkaTopicProducer()
        flist = [2, 3, 6, 7, 19]
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as excuter:
            futures = []
            # Submit tasks to the executor for each filter
            for f in flist:
                params.update({"typeIds": f})
                paramscopy = params.copy()
                futures.append(
                    excuter.submit(
                        getFilter, session, paramscopy, producer, onlyid=onlyid
                    )
                )
            for f in futures:
                print(f)
    finally:
        session.__del__()


def main_scraper(payload):
    adtype = payload["real_state_type"]
    if adtype == "Updated/Latest Ads":
        UpdateAvendrealouer()
    elif payload.get("real_state_type") == "deletedCheck":
        rescrapActiveId()
    else:
        main(adtype)


def getLastUpdates():
    try:
        with open(f"{cpath}/lastUpdate.json", "r") as file:
            updates = json.load(file)
    except:
        return {}
    return updates


def GetAdUpdate(ad):
    updates = {}
    modifiedtime = ad.get("insertDate") or ad.get("releaseDate")
    id = ad.get("id")
    nowtime = datetime.now()
    updates = {
        "timestamp": nowtime.timestamp(),
        "lastupdate": getTimeStamp(modifiedtime),
        "lastadId": id,
    }
    return updates


def CreatelastupdateLog(session, typ):
    params = gparams.copy()
    updates = getLastUpdates()
    if typ == "rental":
        catid = "2"
    else:
        catid = "1,3"
    params.update({"sorts[0].Name": "_newest", "transactionIds": catid, "pageSize": 1})
    d = fetch(session, url, params)
    try:
        data = d["items"][0]
        latupdate = GetAdUpdate(data)
        updates.update({typ: latupdate})
    except Exception as e:
        traceback.print_exc()
        print("execption ======>", e)

    print(updates)
    with open(f"{cpath}/lastUpdate.json", "w") as file:
        file.write(json.dumps(updates))


def asyncUpdateAvendrealouer():
    """
    Asynchronously updates Avendrealouer data based on the last updates.

    Returns:
        None
    """
    params = gparams.copy()
    updates = getLastUpdates()
    try:
        session = HttpRequest(
            True,
            "https://ws-web.avendrealouer.fr/",
            headers,
            {},
            {},
            False,
            cpath,
            1,
            10,
        )
        if not updates:
            # Create last update logs for "rental" and "sale"
            CreatelastupdateLog(session, "rental")
            CreatelastupdateLog(session, "sale")
        updates = getLastUpdates()
        for key, val in updates.items():
            CreatelastupdateLog(session, key)
            if key == "rental":
                catid = "2"
            else:
                catid = "1,3"
            params.update(
                {
                    "sorts[0].Name": "_newest",
                    "transactionIds": catid,
                    "pageSize": 100,
                }
            )
            updated = False
            p = 1
            producer = AsyncKafkaTopicProducer()
            while not updated and p * pagesize <= 700:
                print(f"cheking page {p}")
                # Parse items and save data
                adsres = parstItems(
                    session, params, page=p, save=False, producer=producer
                )
                ads = adsres["items"]
                lastad = ads[len(ads) - 1]
                webupdates = GetAdUpdate(lastad)
                savedata(adsres, producer=producer)
                res = val["lastupdate"] > webupdates["lastupdate"]
                print(
                    f"{val['lastupdate']}>{webupdates['lastupdate']} ={res} and type {key}"
                )
                if res:
                    updated = True
                p += 1
    finally:
        session.__del__()


def rescrapActiveId():
    """
    Rescrapes active IDs and updates the last check time.

    Returns:
        None
    """
    nowtime = datetime.now()
    nowtime = nowtime - timedelta(hours=1)
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as excuter:
        futures = excuter.map(main, ["rental", "sale"], [True, True])
        for f in futures:
            print(f)
    saveLastCheck(website, nowtime.isoformat())


def UpdateAvendrealouer():
    """
    Updates Avendrealouer data.

    Returns:
        None
    """
    asyncUpdateAvendrealouer()


if __name__ == "__main__":
    url = "https://ws-web.avendrealouer.fr/realestate/properties/"
    asyncio.run(main())
