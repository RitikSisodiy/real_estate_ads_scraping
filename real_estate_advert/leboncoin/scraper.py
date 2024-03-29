import asyncio
import random
import threading
import traceback
from urllib import response
import urllib.request
import aiohttp
import json
from datetime import datetime, timedelta
import time
import os
import gzip
import requests, settings


from HttpRequest.AioProxy import ProxyScraper
from saveLastChaeck import saveLastCheck
from .parser import ParseLeboncoin
from aiosocksy.connector import ProxyConnector, ProxyClientRequest
from HttpRequest.uploader import AsyncKafkaTopicProducer

cpath = os.path.dirname(__file__) or "."
producer = AsyncKafkaTopicProducer()
KafkaTopicName = settings.KAFKA_LEBONCOIN
commonAdsTopic = settings.KAFKA_COMMON_PATTERN
website = "leboncoin.com"
commonIdUpdate = f"activeid-{website}"


def now_time_int():
    dateobj = datetime.now()
    total = int(dateobj.strftime("%S"))
    total += int(dateobj.strftime("%M")) * 60
    total += int(dateobj.strftime("%H")) * 60 * 60
    total += (int(dateobj.strftime("%j")) - 1) * 60 * 60 * 24
    total += (int(dateobj.strftime("%Y")) - 1970) * 60 * 60 * 24 * 365
    return total


def saveAds(res):
    ads = res.get("ads")
    totalads = ""
    for ad in ads:
        totalads += json.dumps(ad) + "\n"
    if totalads:
        with open("out12.json", "a") as file:
            file.write(totalads)


class LeboncoinScraper:
    """
    This class is used to scrape data from Leboncoin.fr.

    Attributes:
        cookies (dict): A dictionary of cookies that will be used to authenticate with Leboncoin.fr.
        headers (dict): A dictionary of headers that will be used to make requests to Leboncoin.fr.
        proxies (list): A list of proxies that will be used to make requests to Leboncoin.fr.
        proxy (dict): The current proxy that is being used.
        asyncSession (dict): A dictionary of asyncio session objects that are used to make requests.
        session (requests.session): A requests session object that is used to make requests.
        parameter (dict): A dictionary of parameters that will be used to search for listings on Leboncoin.fr.
        autoSave (bool): Whether or not to automatically save the scraped data to a file.
        outputfile (str): The name of the file that the scraped data will be saved to.
        searchurl (str): The URL of the API endpoint that is used to search for listings on Leboncoin.fr.
        singleUrl (str): The URL of the API endpoint that is used to get the details of a single listing on Leboncoin.fr.
    """

    def __init__(self) -> None:
        pass

    async def init(self, parameter=None, outputfilename=None, proxyThread=True) -> None:
        self.pc = 0
        self.pcd = {}
        try:
            self.cookies = {}
            with open("cookies.txt", "r") as file:
                cookie = file.read()
                rescookies = rescookies.split(";")
                for cookie in rescookies:
                    try:
                        key, val = cookie.split("=")
                        self.cookies[key] = val
                    except:
                        pass
        except:
            cookie = ""
        self.headers = {
            "X-LBC-CC": "7",
            "Accept": "application/json,application/hal+json",
            "User-Agent": "LBC;Android;8.1.0;Android SDK built for x86;phone;6dcaf32b2836c4f7;wifi;5.70.2;570200;0",
            "Content-Type": "application/json; charset=UTF-8",
            "Host": "api.leboncoin.fr",
            "Connection": "Keep-Alive",
            "Accept-Encoding": "gzip",
        }
        if not self.cookies:
            self.cookies = {
                "datadome": ".B08HCAFRuFBU8GC7hocph0ac~EBB56RXmYV6QWkiBfDXZS--KajhR~7XibDdOz2QwA6A4UL.2P90icl_MZ8vjMRNbO~TB52m-Yve2TPtceRoyPxhZ9MHbP6NW4_4vx9",
                "didomi_token": "eyJ1c2VyX2lkIjoiZGM5YWFkMTctM2UxMC00YjgwLThiNDYtYmFlZTk0NWFhYzU5IiwidmVuZG9ycyI6eyJlbmFibGVkIjpbImM6cm9ja3lvdSIsImM6cHVib2NlYW4tYjZCSk10c2UiLCJjOnJ0YXJnZXQtR2VmTVZ5aUMiLCJjOnNjaGlic3RlZC1NUVBYYXF5aCIsImM6Z3JlZW5ob3VzZS1RS2JHQmtzNCIsImM6cmVhbHplaXRnLWI2S0NreHlWIiwiYzpsZW1vbWVkaWEtemJZaHAyUWMiLCJjOnlvcm1lZGlhcy1xbkJXaFF5UyIsImM6bWF5dHJpY3NnLUFTMzVZYW05IiwiYzpzYW5vbWEiLCJjOnJhZHZlcnRpcy1TSnBhMjVIOCIsImM6cXdlcnRpemUtemRuZ0UyaHgiLCJjOmxiY2ZyYW5jZSIsImM6cmV2bGlmdGVyLWNScE1ucDV4IiwiYzpyZXNlYXJjaC1ub3ciLCJjOndoZW5ldmVybS04Vllod2IyUCIsImM6YWRtb3Rpb24iLCJjOnRoaXJkcHJlc2UtU3NLd21IVksiLCJjOmFmZmlsaW5ldCIsImM6aW50b3dvd2luLXFhenQ1dEdpIiwiYzpkaWRvbWkiLCJjOmpxdWVyeSIsImM6YWthbWFpIiwiYzphYi10YXN0eSIsImM6emFub3giLCJjOm1vYmlmeSIsImM6YXQtaW50ZXJuZXQiLCJjOnB1cnBvc2VsYS0zdzRaZktLRCIsImM6aW5mZWN0aW91cy1tZWRpYSIsImM6bWF4Y2RuLWlVTXROcWNMIiwiYzpjbG91ZGZsYXJlIiwiYzppbnRpbWF0ZS1tZXJnZXIiLCJjOmFkdmFuc2UtSDZxYmF4blEiLCJjOnNuYXBpbmMteWhZbkpaZlQiLCJjOnJldGFyZ2V0ZXItYmVhY29uIiwiYzp0dXJibyIsImM6Y2FibGF0b2xpLW5SbVZhd3AyIiwiYzp2aWFudC00N3gyWWhmNyIsImM6dnVibGUtY01DSlZ4NGUiLCJjOmJyYW5jaC1WMmRFQlJ4SiIsImM6c2ZyLU1kcGk3a2ZOIiwiYzphcHBzZmx5ZXItWXJQZEdGNjMiLCJjOmhhc29mZmVyLThZeU1UdFhpIiwiYzpsa3FkLWNVOVFtQjZXIiwiYzpzd2F2ZW4tTFlCcmltQVoiLCJjOmZvcnR2aXNpb24taWU2YlhUdzkiLCJjOmFkaW1vLVBoVVZtNkZFIiwiYzpvc2Nhcm9jb20tRlJjaE5kbkgiLCJjOnJldGVuY3ktQ0xlclppR0wiLCJjOmlsbHVtYXRlYy1DaHRFQjRlayIsImM6YWRsaWdodG5pLXRXWkdyZWhUIiwiYzpyb2NrZXJib3gtZlRNOEVKOVAiXSwiZGlzYWJsZWQiOltdfSwicHVycG9zZXMiOnsiZW5hYmxlZCI6WyJjb29raWVzIiwic2VsZWN0X2Jhc2ljX2FkcyIsImNyZWF0ZV9hZHNfcHJvZmlsZSIsIm1lYXN1cmVfYWRfcGVyZm9ybWFuY2UiLCJtYXJrZXRfcmVzZWFyY2giLCJpbXByb3ZlX3Byb2R1Y3RzIiwic2VsZWN0X3BlcnNvbmFsaXplZF9hZHMiLCJnZW9sb2NhdGlvbl9kYXRhIiwicGVyc29ubmFsaXNhdGlvbm1hcmtldGluZyIsInByaXgiLCJtZXN1cmVhdWRpZW5jZSIsImV4cGVyaWVuY2V1dGlsaXNhdGV1ciJdLCJkaXNhYmxlZCI6W119fQ==",
                "__Secure-InstanceId": "002a7e78-40ec-4587-9161-e29c293bbe84",
            }
        LEBONCOIN_CHECK_URL = "https://api.leboncoin.fr/finder/classified/2130999715"
        self.prox = ProxyScraper(LEBONCOIN_CHECK_URL, self.headers, True)
        try:
            self.proxies = self.readProxy()
            if not self.proxies:
                await self.waitgetProxyList()
        except:
            await self.waitgetProxyList()
        self.proxy = {}
        self.asyncSession = {}
        await self.changeSessionProxy()
        if proxyThread:
            self.proxyUpdateThread()
        self.session = requests.session()
        self.parameter = parameter
        self.autoSave = True
        self.outputfile = outputfilename
        self.searchurl = "https://api.leboncoin.fr/api/adfinder/v1/search"
        self.singleUrl = "https://api.leboncoin.fr/finder/classified"

    def threadsleep(self, t):
        while t > 0:
            if not self.startThread:
                return True
            time.sleep(1)
            t -= 1
        return False

    def updateProxyList(self, interval=300):
        if self.readProxy() and self.threadsleep(interval):
            return
        while self.startThread:
            self.getProxyList()
            b = self.threadsleep(interval)
            if b:
                break
        print("thread is stopped")

    def proxyUpdateThread(self):
        print("proxy thread is started")
        self.startThread = True
        self.proc = threading.Thread(target=self.updateProxyList, args=())
        self.proc.daemon = True
        self.proc.start()

    def readProxy(self):
        with open(f"{cpath}/working.txt", "r") as file:
            proxies = file.readlines()
        return [json.loads(proxy) for proxy in proxies]

    async def getRandomProxy(self):
        try:
            proxy = random.choice(self.proxies)
            return proxy
        except:
            await asyncio.sleep(30)
            await self.waitgetProxyList()
            return await self.getRandomProxy()

    def __del__(self):
        self.startThread = False

    def getProxyList(self):
        self.prox.FetchNGetProxy()
        self.prox.save(cpath)
        self.proxies = self.readProxy()

    async def waitgetProxyList(self):
        # Asynchronously fetch and get the proxy list
        await self.prox.main()
        self.prox.save(cpath)
        self.proxies = self.readProxy()

    def proxyUpdateThread(self):
        # Start a thread to update the proxy list
        print("proxy thread is started")
        self.startThread = True
        self.proc = threading.Thread(target=self.updateProxyList, args=())
        self.proc.daemon = True
        self.proc.start()

    async def fetch(self, url, method="POST", **kwargs):
        """
        Fetches the data from the specified URL.

        Args:
            url (str): The URL of the resource to fetch.
            method (str): The HTTP method to use.
            **kwargs: Keyword arguments that will be passed to the requests library.

        Returns:
            The response data.

        Raises:
            Exception: If an error occurs.
        """
        kwargs["ssl"] = False
        if not self.asyncSession.get(self.pc):
            await self.changeSessionProxy()
        if kwargs.get("headers"):
            kwargs["headers"] = {
                **kwargs.get("headers"),
                "cookies": self.proxy[self.pc]["cookies"],
            }
        try:
            print(self.proxy[self.pc])
            if method == "GET":
                async with self.asyncSession[self.pc].get(
                    url, proxy=self.proxy[self.pc]["http"], timeout=15, **kwargs
                ) as r:
                    if r.status == 200:
                        return True
                    if r.status == 410 or r.status == 404:
                        return False
                    return await self.fetch(url, method=method, **kwargs)
            async with self.asyncSession[self.pc].post(
                url, proxy=self.proxy[self.pc]["http"], timeout=15, **kwargs
            ) as r:
                print(r)
                if r.status == 200:
                    return await r.json()
                else:
                    await self.changeSessionProxy()
                    await asyncio.sleep(1)
                    return await self.fetch(url, method=method, **kwargs)
        except Exception as e:
            traceback.print_exc()
            print(e, "<============= exception")
            await asyncio.sleep(5)
            await self.changeSessionProxy()
            return await self.fetch(url, method=method, **kwargs)

    async def changeSessionProxy(self):
        try:
            await self.asyncSession[self.pc].close()
        except:
            pass
        self.proxy[self.pc] = await self.getRandomProxy()
        connector = ProxyConnector()
        self.asyncSession[self.pc] = aiohttp.ClientSession(
            connector=connector, request_class=ProxyClientRequest
        )

    def updateCookies(self):
        self.headers["Cookie"] = ";".join(
            [(f"{key}={val}") for key, val in self.cookies.items()]
        )
        with open("cookies.txt", "w") as file:
            file.write(self.headers["Cookie"])

    async def CheckIfAdIdLive(self, id):
        res = await self.fetch(
            f"{self.singleUrl}/{id}", headers=self.headers, method="GET"
        )
        return res

    async def CrawlLeboncoin(self, onlyid=False):
        # Crawl Leboncoin by making a POST request with the given parameters
        parameter = self.parameter
        res = await self.fetch(
            "https://api.leboncoin.fr/finder/search",
            headers=self.headers,
            json=parameter,
        )
        self.pc = (self.pc + 1) % 5
        if self.autoSave:
            await self.saveAds(res, onlyid=onlyid)
        return res

    def checkNext(self, res):
        # Check if there is a next page in the response
        try:
            next = res["pivot"]
            self.parameter["pivot"] = next
            return next
        except:
            return False

    async def IntCrawling(self, onlyid=False):
        self.autoSave = True
        res = await self.CrawlLeboncoin(onlyid=onlyid)
        totalcount = res.get("total")
        scrapedCount = len(res.get("ads"))
        nextpage = self.checkNext(res)
        while nextpage and scrapedCount <= totalcount:
            print(nextpage)
            self.parameter["pivot"] = nextpage
            res = await self.CrawlLeboncoin(onlyid=onlyid)
            scrapedCount += len(res.get("ads"))
            nextpage = self.checkNext(res)

    async def saveAds(self, res, onlyid=False):
        ads = res.get("ads")
        if not ads:
            return 0
        if onlyid:
            ads = parseAds = [ParseLeboncoin(ad) for ad in ads]
            await producer.TriggerPushDataList_v1(commonIdUpdate, ads)
        else:
            await producer.TriggerPushDataList(KafkaTopicName, ads)
            parseAds = [ParseLeboncoin(ad) for ad in ads]
            await producer.TriggerPushDataList(commonAdsTopic, parseAds)

    async def UpdataAds(self):
        self.autoSave = False
        try:
            with open(f"{cpath}/lastUpdate.json", "r") as file:
                lastUpdate = json.load(file)
            lastTime = self.getTimeStamp(lastUpdate["addUploadTime"])
            updated = False
        except Exception as e:
            print(e)
            print("please check your lastUpdate.json file")
            raise e
        first = True
        while not updated:
            res = {"ads": []}
            resdata = await self.CrawlLeboncoin()
            for data in resdata["ads"]:
                if first:
                    nowtime = datetime.now()
                    nowtime = nowtime.timestamp()
                    lastupdate = {
                        "time": nowtime,
                        "source": data["url"],
                        "adid": data["list_id"],
                        "addUploadTime": data["index_date"],
                    }
                    first = False
                    with open(f"{cpath}/lastUpdate.json", "w") as file:
                        file.write(json.dumps(lastupdate))
                if lastTime < self.getTimeStamp(data["index_date"]):
                    res["ads"].append(data)
                else:
                    print("very old add", data)
                    updated = True
                    break
            await self.saveAds(res)
            nextpage = self.checkNext(resdata)
            if not nextpage:
                print("no pages left")
                break

    def getTimeStamp(self, strtime):
        formate = "%Y-%m-%d %H:%M:%S"
        t = datetime.strptime(strtime, formate)
        return t.timestamp()


async def updateLebonCoin():
    data = json.load(open(f"{cpath}/filter.json", "r"))
    ob = LeboncoinScraper()
    try:
        await ob.init(data, "newout1")
        await ob.UpdataAds()
    finally:
        ob.__del__()


async def CheckId(id):
    ob = LeboncoinScraper()
    await ob.init(proxyThread=False)
    status = await ob.CheckIfAdIdLive(id)
    ob.__del__()
    return status


async def ScrapLebonCoin(data={}, onlyid=False):
    if not data:
        data = json.load(open(f"{cpath}/filter.json", "r"))
    ob = LeboncoinScraper()
    try:
        await ob.init(data, "newout1")
        await ob.IntCrawling(onlyid=onlyid)
    finally:
        ob.__del__()


def leboncoinAdScraper(payload):
    typ = payload.get("real_state_type")
    if not typ:
        id = payload.get("id")
        return asyncio.run(CheckId(id))
    if typ == "Updated/Latest Ads":
        asyncio.run(updateLebonCoin())
    elif payload.get("real_state_type") == "deletedCheck":
        rescrapActiveId()
    else:
        asyncio.run(ScrapLebonCoin())


def rescrapActiveIdbyType():
    data = json.load(open(f"{cpath}/filter.json", "r"))
    asyncio.run(ScrapLebonCoin(data=data, onlyid=True))


def rescrapActiveId():
    nowtime = datetime.now()
    nowtime = nowtime - timedelta(hours=1)
    try:
        rescrapActiveIdbyType()
    finally:
        saveLastCheck(website, nowtime.isoformat())


if __name__ == "__main__":
    data = json.load(open(f"{cpath}/filter.json", "r"))
    ob = LeboncoinScraper(data, "newout1")
    ob.UpdataAds()
