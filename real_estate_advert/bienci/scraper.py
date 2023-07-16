from datetime import datetime, timedelta
import json
import os
import urllib.parse
import traceback
import requests
import concurrent.futures
from HttpRequest.requestsModules import HttpRequest
from .parser import ParseBienici
from requests_html import HTMLSession
import settings
from HttpRequest.uploader import AsyncKafkaTopicProducer

try:
    from fetch import fetch
except:
    from .fetch import fetch
pageSize = 499
website = "bienici.com"
kafkaTopicName = settings.KAFKA_BIENICI
commanTopicName = settings.KAFKA_COMMON_PATTERN
commonIdUpdate = f"activeid-{website}"
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
        Filter["from"] = Filter["size"] * page + 1
    Filter = json.dumps(Filter)

    Filter = {"filters": Filter}
    Filter = urllib.parse.urlencode(Filter)
    url = f"https://www.bienici.com/realEstateAds.json?{Filter}&extensionType=extendedIfNoResult"
    return url


def GetLast(val):
    try:
        return val[len(val) - 1]
    except:
        return None


def getFirst(val):
    try:
        return val[0]
    except:
        return None


def syncfetch(session, url, Json=False, **kwargs):
    """
    Performs a synchronous HTTP GET request using the provided session object.

    Args:
    - session: A session object for making HTTP requests.
    - url: A string representing the URL to request.
    - Json: A boolean indicating whether to return the response as JSON or not.
    - kwargs: Additional keyword arguments to pass to the `session.get` method.

    Returns:
    - The HTTP response object or the JSON response, depending on the value of `Json`.
    """
    kwargs["headers"] = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36",
    }
    res = session.get(url, **kwargs)
    if res.status_code == 200:
        if Json:
            return res.json()
        else:
            return res
    else:
        print(res.status_code)
        print(url)
        return syncfetch(session, url, Json=Json, **kwargs)


def getTotalResult(session, parameter):
    """
    Retrieves the total number of real estate ads matching the provided filter parameters.

    Args:
    - session: A session object for making HTTP requests.
    - parameter: A dictionary representing the filter parameters.

    Returns:
    - totalres: An integer representing the total number of matching real estate ads.
      If the total is not available, it returns 0.
    """
    (
        parameter["sortBy"],
        parameter["sortOrder"],
        parameter["size"],
        parameter["from"],
    ) = ("price", "desc", 1, 0)
    url = getFilterUrl(parameter)
    res = syncfetch(session, url, Json=True)
    totalres = res.get("total")
    if totalres:
        return totalres
    else:
        return 0


def getMax(session, parameter, max="price"):
    # Retrieves the maximum value of a specific property (e.g., price) for real estate ads matching the provided filter parameters.
    parameter["sortBy"], parameter["sortOrder"], parameter["size"] = max, "desc", 1
    url = getFilterUrl(parameter)
    res = syncfetch(session, url, Json=True)
    totalres = res.get("realEstateAds")[0].get("price")
    if totalres:
        return totalres
    else:
        return 0


def readGenFilter():
    # Reads the generated prize filter from a JSON file.
    try:
        with open("generatedPrizeFilter.json", "r") as file:
            data = json.load(file)
        return data
    except:
        return {}


def writeGenFilter(key, value):
    # Writes the generated prize filter onto a file
    prev = readGenFilter()
    prev[key] = value
    with open("generatedPrizeFilter.json", "w") as file:
        file.write(json.dumps(prev))


def genFilter(
    parameter, typ, onlyid=False, low="minPrice", max="maxPrice", session=None
):
    """
    Generates and applies filters to retrieve real estate ads based on the provided parameters.

    Args:
    - parameter: A dictionary representing the filter parameters.
    - typ: A string representing the filter type.
    - onlyid (optional): A boolean indicating whether to retrieve only the IDs of the matching ads (default is False).
    - low (optional): A string representing the lower bound property for the filter (default is "minPrice").
    - max (optional): A string representing the upper bound property for the filter (default is "maxPrice").
    - session (optional): A session object for making HTTP requests (default is None).

    Returns:
    - None
    """
    parameter["filterType"] = typ
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36",
    }
    session2 = HttpRequest(
        True,
        'https://www.bienici.com/realEstateAds.json?filters={"size":"1"}',
        {},
        headers,
        {},
        False,
        cpath,
        1,
        10,
    )
    session = requests.session()
    dic = parameter.copy()
    totalresult = getTotalResult(session, dic)
    acres = totalresult

    if low == "minPrice":
        iniinterval = [0, 1000]
        maxprize = getMax(session, dic.copy())
    else:
        maxprize = getMax(session, dic.copy(), max="surfaceArea")
        iniinterval = [0, 1]

    maxresult = 4800
    filterurllist = ""
    finalresult = 0
    nooffilter = 0
    while iniinterval[1] <= maxprize:
        dic[low], dic[max] = iniinterval
        totalresult = getTotalResult(session, dic)
        if totalresult <= 4800 and totalresult > 0:
            filterurllist += json.dumps(iniinterval) + "/n/:"
            FetchFilter(dic, onlyid, session2)
            print("going to next")
            iniinterval[0] = iniinterval[1] + 1
            iniinterval[1] = iniinterval[0] + int(iniinterval[0] / 2)
            finalresult += totalresult
            nooffilter += 1
        elif acres - finalresult < 4800 and acres - finalresult > 0:
            print(maxresult, acres, maxresult - acres)

            last = 10
            iniinterval[1] = maxprize
        elif totalresult == 0:
            last = 10
            iniinterval[0] = iniinterval[1]
            iniinterval[1] = iniinterval[0] + (int(iniinterval[1] / last) or 1)

        elif (
            iniinterval[1] - iniinterval[0] <= 2
            and totalresult > maxresult
            and low == "minPrice"
        ):
            finalresult += totalresult
            genFilter(dic.copy(), typ, onlyid, "minArea", "maxArea", session)
            iniinterval[0] = iniinterval[1]
            iniinterval[1] += 1
        elif totalresult > maxresult:
            last = -5
            dif = iniinterval[1] - iniinterval[0]
            iniinterval[1] = iniinterval[1] + int(dif / -2)
            if iniinterval[0] > iniinterval[1]:
                iniinterval[1] = iniinterval[0] + 1

        print(
            f"\r{totalresult}-{maxresult}::::{acres} of  {finalresult}==>{iniinterval} {maxprize} {low} no of filter",
            end="",
        )

    filterurllist += json.dumps(iniinterval)
    finalresult += totalresult
    print(finalresult, acres)
    filterurllist = [json.loads(query) for query in filterurllist.split("/n/:")]
    writeGenFilter(typ, filterurllist)


asyncpage = 10


def saveRealstateAds(ads, **kwargs):
    producer = kwargs.get("producer")
    if kwargs.get("onlyid"):
        ads = [ParseBienici(ad) for ad in ads]
        producer.PushDataList_v1(commonIdUpdate, ads)
    else:
        producer.PushDataList(kafkaTopicName, ads)
        ads = [ParseBienici(ad) for ad in ads]
        producer.PushDataList(commanTopicName, ads)


    # allads = ''
    # for ad in ads:
    #     allads+= json.dumps(ad)+"\n"
    # with open("output/output.json",'a') as file:
    #     file.write(allads)
def getPage(total,size):
    totalpage = total/size
    totalpage = int(totalpage)+1 if totalpage>int(totalpage) else int(totalpage)
    totalpage = 6 if totalpage>6 else totalpage
    return totalpage


def GetAllPages(baseurl, session, first=False, Filter=None, save=True, **kwargs):
    print(Filter)
    r = fetch(baseurl, session, Json=True)
    if r:
        print(f"from===========>{r['from']}")
        if save:
            ads = r["realEstateAds"]
            saveRealstateAds(ads, **kwargs)
        else:
            return r["realEstateAds"]
        if first:
            totalpage = getPage(r["total"], Filter["size"])

            tasks = []
            print(totalpage, r["total"], Filter["size"])

            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as excuter:
                baseurl = [getFilterUrl(Filter, page=i) for i in range(1, totalpage)]
                futures = []

                for i in range(0, len(baseurl)):
                    futures.append(
                        excuter.submit(
                            GetAllPages, baseurl[i], session, False, **kwargs
                        )
                    )
                if r["total"] > 2400 and Filter["sortOrder"] == "desc":
                    Filter["sortOrder"] = "asc"
                    if Filter["sortOrder"] == "asc":
                        r["total"] -= 2400
                        totalpage = getPage(r["total"], Filter["size"])
                        futures += [
                            excuter.submit(GetAllPages, url, session, False, **kwargs)
                            for url in [
                                getFilterUrl(Filter, page=i)
                                for i in range(0, totalpage)
                            ]
                        ]
                for f in futures:
                    print(f)


def FetchFilter(filters, onlyid=False, session=None):
    """
    Fetches real estate ads based on the provided filters.

    Args:
    - filters: A dictionary representing the filter parameters.
    - onlyid (optional): A boolean indicating whether to retrieve only the IDs of the matching ads (default is False).
    - session (optional): A session object for making HTTP requests (default is None).

    Returns:
    - None
    """
    filters["size"] = 400
    if not session:
        headers = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36",
        }
        session = HttpRequest(
            True,
            'https://www.bienici.com/realEstateAds.json?filters={"size":"1"}',
            {},
            headers,
            {},
            False,
            cpath,
            1,
            10,
        )
    try:
        baseurl = getFilterUrl(filters)
        producer = AsyncKafkaTopicProducer()

        GetAllPages(
            baseurl,
            session,
            first=True,
            Filter=filters,
            producer=producer,
            onlyid=onlyid,
        )
    finally:
        session.__del__()


def getLastUpdates():
    try:
        with open(f"{cpath}/lastUpdate.json", "r") as file:
            updates = json.load(file)
    except:
        return {}
    return updates


def getTimeStamp(strtime):
    formate = "%Y-%m-%dT%H:%M:%S.%fZ"

    t = datetime.strptime(strtime, formate)
    return t.timestamp()


def GetAdUpdate(ad):
    nowtime = datetime.now()
    update = {
        "timestamp": nowtime.timestamp(),
        "lastupdate": getTimeStamp(ad["modificationDate"]),
        "lastadId": ad["id"],
    }
    return update


def CreatelastupdateLog(session, typ):
    # Creates the log for last update time of ads
    updates = getLastUpdates()
    if typ == "sale" or typ == "buy":
        typ = "buy"
    else:
        typ = "rent"
    param = {
        "size": pageSize,
        "from": 0,
        "showAllModels": False,
        "propertyType": ["house", "flat"],
        "filterType": typ,
        "newProperty": False,
        "page": 1,
        "sortBy": "publicationDate",
        "sortOrder": "desc",
        "onTheMarket": [True],
    }
    url = getFilterUrl(param)
    print("fetching url :", url)
    d = fetch(url, session, Json=True)

    try:
        ad = d["realEstateAds"][0]
        latupdate = GetAdUpdate(ad)

        updates.update({typ: latupdate})
    except Exception as e:
        traceback.print_exc()
        print("execption ======>", e)

    print(updates)
    with open(f"{cpath}/lastUpdate.json", "w") as file:
        file.write(json.dumps(updates))


def asyncUpdateBienci():
    """
    Asynchronously updates Bienci data based on the last updates.

    Returns:
        None
    """
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36",
    }
    session = HttpRequest(
        True,
        'https://www.bienici.com/realEstateAds.json?filters={"size":"1"}',
        {},
        headers,
        {},
        False,
        cpath,
        1,
        10,
    )
    res = ""
    try:
        producer = AsyncKafkaTopicProducer()

        param = {
            "size": 26,
            "from": 0,
            "showAllModels": False,
            "propertyType": ["house", "flat"],
            "newProperty": False,
            "page": 1,
            "sortBy": "publicationDate",
            "sortOrder": "desc",
            "onTheMarket": [True],
        }
        updates = getLastUpdates()
        print(updates)
        if updates:
            CreatelastupdateLog(session, "rent")
            CreatelastupdateLog(session, "buy")
            for key, val in updates.items():
                param.update({"filterType": key})
                updated = False
                page = 1
                while not updated and page * param["size"] < 2400:
                    url = getFilterUrl(param, page=page)
                    r = fetch(url, session, Json=True)
                    ads = r["realEstateAds"]
                    adslist = []
                    for ad in ads:
                        adtime = getTimeStamp(ad["modificationDate"])
                        print(
                            f"{val['lastupdate']}<{adtime} = ",
                            val["lastupdate"] < getTimeStamp(ad["modificationDate"]),
                        )
                        if val["lastupdate"] < getTimeStamp(ad["modificationDate"]):
                            adslist.append(ad)
                        else:
                            msg = f"{len(adslist)} {key} new ads scraped"
                            res += " " + msg
                            print(msg)
                            updated = True
                            break
                    saveRealstateAds(adslist, producer=producer)
                    page += 1

        else:
            CreatelastupdateLog(session, "rent")
            CreatelastupdateLog(session, "buy")
    finally:
        session.__del__()
    return res


def CheckId(id):
    session = HTMLSession()
    url = f"https://www.bienici.com/realEstateAd.json?id={id}"
    res = fetch(url, session, Json=True)
    return bool(res)


def UpdateBienci():
    return asyncUpdateBienci()


from concurrent.futures import ThreadPoolExecutor
from saveLastChaeck import saveLastCheck


def rescrapActiveIdbyType(Type):
    param = {
        "size": pageSize,
        "from": 0,
        "showAllModels": False,
        "filterType": "buy",
        "propertyType": ["house", "flat"],
        "newProperty": False,
        "page": 1,
        "sortBy": "relevance",
        "sortOrder": "desc",
        "onTheMarket": [True],
    }

    genFilter(param, Type, True)


def rescrapActiveId():
    """
    Rescrapes active IDs and updates the last check time.

    Returns:
        None
    """
    nowtime = datetime.now()
    nowtime = nowtime - timedelta(hours=1)

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as excuter:
        futures = [excuter.submit(rescrapActiveIdbyType, i) for i in ["buy", "rent"]]
        for f in futures:
            print(f)
    print("complited")
    saveLastCheck(website, nowtime.isoformat())


def main_scraper(payload):
    """
    Main function to process ads data based on the provided ads type.

    Args:
        payload: Dictionary containing types of ads to scrape(rental, sale, deleted etc)

    Returns:
        None
    """
    if payload.get("real_state_type") == "Updated/Latest Ads":
        res = UpdateBienci()
        return res
    elif payload.get("real_state_type") == "deletedCheck":
        rescrapActiveId()
    else:
        param = {
            "size": pageSize,
            "from": 0,
            "showAllModels": False,
            "filterType": "buy",
            "propertyType": ["house", "flat"],
            "newProperty": False,
            "page": 1,
            "sortBy": "relevance",
            "sortOrder": "desc",
            "onTheMarket": [True],
        }
        genFilter(param, typ="buy")
        genFilter(param, typ="rent")


if __name__ == "__main__":
    param = {
        "size": pageSize,
        "from": 0,
        "showAllModels": False,
        "filterType": "buy",
        "propertyType": ["house", "flat"],
        "newProperty": False,
        "page": 1,
        "sortBy": "relevance",
        "sortOrder": "desc",
        "onTheMarket": [True],
    }
    genFilter(param, typ="buy")
    genFilter(param, typ="rent")
