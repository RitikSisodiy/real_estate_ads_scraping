import asyncio
import random
import threading
import traceback
from urllib import response
import urllib.request
import aiohttp
from datetime import datetime
import json
from datetime import datetime
import time
import os
import gzip
import requests

# from .scrapProxy import ProxyScraper
from HttpRequest.AioProxy import ProxyScraper
from .parser import ParseLeboncoin
# from aiohttp_socks import ProxyType, ProxyConnector, ChainProxyConnector
from aiosocksy.connector import ProxyConnector, ProxyClientRequest
from HttpRequest.uploader import AsyncKafkaTopicProducer
cpath =os.path.dirname(__file__) or "."
producer = AsyncKafkaTopicProducer()
KafkaTopicName = 'leboncoin-data_v1'
commonAdsTopic= "common-ads-data_v1"
def now_time_int():
    dateobj = datetime.now()
    total = int(dateobj.strftime('%S'))
    total += int(dateobj.strftime('%M')) * 60
    total += int(dateobj.strftime('%H')) * 60 * 60
    total += (int(dateobj.strftime('%j')) - 1) * 60 * 60 * 24
    total += (int(dateobj.strftime('%Y')) - 1970) * 60 * 60 * 24 * 365
    return total
def saveAds(res):
    ads = res.get('ads')
    # producer.PushDataList('leboncoin-data_v1',ads)
    totalads = ''
    for ad in ads:
        totalads+= json.dumps(ad)+"\n"
    if totalads:
        with open('out12.json','a') as file:
            file.write(totalads)

class LeboncoinScraper:
    def __init__(self) -> None:
        pass
    async def init(self,parameter=None,outputfilename=None,proxyThread=True) -> None:
        try:
            self.cookies  = {}
            with open("cookies.txt",'r') as file:
                cookie = file.read()
                rescookies = rescookies.split(";")
                for cookie in rescookies:
                    try:
                        key,val = cookie.split("=")
                        self.cookies[key] = val
                    except:pass
        except:
            cookie = ""
        self.headers = {
            "X-LBC-CC": "7",
            "Accept": "application/json,application/hal+json",
            "User-Agent": "LBC;Android;8.1.0;Android SDK built for x86;phone;6dcaf32b2836c4f7;wifi;5.70.2;570200;0",
            "Content-Type": "application/json; charset=UTF-8",
            "Host": "api.leboncoin.fr",
            "Connection": "Keep-Alive",
            "Accept-Encoding": "gzip"
            }
        if not self.cookies:
            self.cookies = {
                "datadome":".B08HCAFRuFBU8GC7hocph0ac~EBB56RXmYV6QWkiBfDXZS--KajhR~7XibDdOz2QwA6A4UL.2P90icl_MZ8vjMRNbO~TB52m-Yve2TPtceRoyPxhZ9MHbP6NW4_4vx9",
                "didomi_token":"eyJ1c2VyX2lkIjoiZGM5YWFkMTctM2UxMC00YjgwLThiNDYtYmFlZTk0NWFhYzU5IiwidmVuZG9ycyI6eyJlbmFibGVkIjpbImM6cm9ja3lvdSIsImM6cHVib2NlYW4tYjZCSk10c2UiLCJjOnJ0YXJnZXQtR2VmTVZ5aUMiLCJjOnNjaGlic3RlZC1NUVBYYXF5aCIsImM6Z3JlZW5ob3VzZS1RS2JHQmtzNCIsImM6cmVhbHplaXRnLWI2S0NreHlWIiwiYzpsZW1vbWVkaWEtemJZaHAyUWMiLCJjOnlvcm1lZGlhcy1xbkJXaFF5UyIsImM6bWF5dHJpY3NnLUFTMzVZYW05IiwiYzpzYW5vbWEiLCJjOnJhZHZlcnRpcy1TSnBhMjVIOCIsImM6cXdlcnRpemUtemRuZ0UyaHgiLCJjOmxiY2ZyYW5jZSIsImM6cmV2bGlmdGVyLWNScE1ucDV4IiwiYzpyZXNlYXJjaC1ub3ciLCJjOndoZW5ldmVybS04Vllod2IyUCIsImM6YWRtb3Rpb24iLCJjOnRoaXJkcHJlc2UtU3NLd21IVksiLCJjOmFmZmlsaW5ldCIsImM6aW50b3dvd2luLXFhenQ1dEdpIiwiYzpkaWRvbWkiLCJjOmpxdWVyeSIsImM6YWthbWFpIiwiYzphYi10YXN0eSIsImM6emFub3giLCJjOm1vYmlmeSIsImM6YXQtaW50ZXJuZXQiLCJjOnB1cnBvc2VsYS0zdzRaZktLRCIsImM6aW5mZWN0aW91cy1tZWRpYSIsImM6bWF4Y2RuLWlVTXROcWNMIiwiYzpjbG91ZGZsYXJlIiwiYzppbnRpbWF0ZS1tZXJnZXIiLCJjOmFkdmFuc2UtSDZxYmF4blEiLCJjOnNuYXBpbmMteWhZbkpaZlQiLCJjOnJldGFyZ2V0ZXItYmVhY29uIiwiYzp0dXJibyIsImM6Y2FibGF0b2xpLW5SbVZhd3AyIiwiYzp2aWFudC00N3gyWWhmNyIsImM6dnVibGUtY01DSlZ4NGUiLCJjOmJyYW5jaC1WMmRFQlJ4SiIsImM6c2ZyLU1kcGk3a2ZOIiwiYzphcHBzZmx5ZXItWXJQZEdGNjMiLCJjOmhhc29mZmVyLThZeU1UdFhpIiwiYzpsa3FkLWNVOVFtQjZXIiwiYzpzd2F2ZW4tTFlCcmltQVoiLCJjOmZvcnR2aXNpb24taWU2YlhUdzkiLCJjOmFkaW1vLVBoVVZtNkZFIiwiYzpvc2Nhcm9jb20tRlJjaE5kbkgiLCJjOnJldGVuY3ktQ0xlclppR0wiLCJjOmlsbHVtYXRlYy1DaHRFQjRlayIsImM6YWRsaWdodG5pLXRXWkdyZWhUIiwiYzpyb2NrZXJib3gtZlRNOEVKOVAiXSwiZGlzYWJsZWQiOltdfSwicHVycG9zZXMiOnsiZW5hYmxlZCI6WyJjb29raWVzIiwic2VsZWN0X2Jhc2ljX2FkcyIsImNyZWF0ZV9hZHNfcHJvZmlsZSIsIm1lYXN1cmVfYWRfcGVyZm9ybWFuY2UiLCJtYXJrZXRfcmVzZWFyY2giLCJpbXByb3ZlX3Byb2R1Y3RzIiwic2VsZWN0X3BlcnNvbmFsaXplZF9hZHMiLCJnZW9sb2NhdGlvbl9kYXRhIiwicGVyc29ubmFsaXNhdGlvbm1hcmtldGluZyIsInByaXgiLCJtZXN1cmVhdWRpZW5jZSIsImV4cGVyaWVuY2V1dGlsaXNhdGV1ciJdLCJkaXNhYmxlZCI6W119fQ==",
                "__Secure-InstanceId":"002a7e78-40ec-4587-9161-e29c293bbe84"
            }
        LEBONCOIN_CHECK_URL = "https://api.leboncoin.fr/finder/classified/2130999715"
        self.prox = ProxyScraper(LEBONCOIN_CHECK_URL,self.headers)
        try:
            self.proxies = self.readProxy()
            if not self.proxies:
                await self.waitgetProxyList()
        except:
            await self.waitgetProxyList()
        self.proxy = await self.getRandomProxy()
        if proxyThread:self.proxyUpdateThread()
        self.session = requests.session()
        # connector = ProxyConnector.from_url("http://lum-customer-c_5afd76d0-zone-residential:7nuh5ts3gu7z@zproxy.lum-superproxy.io:22225")
        self.asyncSession = aiohttp.ClientSession()
        self.parameter = parameter
        # self.updateCookies()
        self.autoSave = True
        self.outputfile = outputfilename
        self.searchurl = "https://api.leboncoin.fr/api/adfinder/v1/search"
        self.singleUrl = "https://api.leboncoin.fr/finder/classified"
    def threadsleep(self,t):
        while(t>0):
            if not self.startThread:
                return True
            time.sleep(1)
            t-=1
        return False
    def updateProxyList(self,interval=300):
        if self.readProxy() and not self.threadsleep(interval):return
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
        with open(f"{cpath}/working.txt","r") as file:
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
        # print("proxy thread is terminated")
    def getProxyList(self):
        self.prox.FetchNGetProxy()
        self.prox.save(cpath)
        self.proxies = self.readProxy()
    async def waitgetProxyList(self):
        await self.prox.main()
        self.prox.save(cpath)
        self.proxies = self.readProxy()
    def proxyUpdateThread(self):
        print("proxy thread is started")
        self.startThread = True
        self.proc = threading.Thread(target=self.updateProxyList, args=())
        self.proc.daemon = True
        self.proc.start()
    async def fetch(self,url,method = "POST",**kwargs):
        try:
            print(self.proxy)
            if method == "GET":
                async with self.asyncSession.get(url,proxy=self.proxy["http"],timeout=15,**kwargs) as r:
                    if r.status==200:
                        return True
                    if r.status == 410 or r.status ==404:
                        return False
                    return await self.fetch(url,method=method,**kwargs)
            async with self.asyncSession.post(url,proxy=self.proxy["http"],timeout=15,**kwargs) as r:
                print(r)
                if r.status==200:
                    return await r.json()
                else:
                    await self.changeSessionProxy()
                    await asyncio.sleep(1)
                    return await self.fetch(url,method=method,**kwargs)
        except Exception as e:
            traceback.print_exc()
            print(e , "<============= exception")
            await asyncio.sleep(5)
            await self.changeSessionProxy()
            return await self.fetch(url,method=method,**kwargs)
    async def changeSessionProxy(self):
        # connector = ProxyConnector.from_url("http://lum-customer-c_5afd76d0-zone-residential:7nuh5ts3gu7z@zproxy.lum-superproxy.io:22225")
        # self.asyncSession.delete()
        await self.asyncSession.close()
        self.proxy = await self.getRandomProxy()
        connector = ProxyConnector()
        self.asyncSession = aiohttp.ClientSession(connector=connector, request_class=ProxyClientRequest)
    def updateCookies(self):
        self.headers['Cookie'] = ";".join([(f"{key}={val}") for key,val in self.cookies.items()])
        with open("cookies.txt",'w') as file:
            file.write(self.headers['Cookie'])
    async def CheckIfAdIdLive(self,id):
        res = await self.fetch(f"{self.singleUrl}/{id}",headers= self.headers,method = "GET")
        return res
    async def CrawlLeboncoin(self):
        parameter= self.parameter
        # headers = {
        #     # cookie=datadome= 
        #     'Cookie': 'didomi_token=eyJ1c2VyX2lkIjoiMmIyN2ZmNDMtODU3NS00MTY4LWFhNzktNDczYjFiMDc4Yjg2IiwidmVuZG9ycyI6eyJlbmFibGVkIjpbImM6cm9ja3lvdSIsImM6cHVib2NlYW4tYjZCSk10c2UiLCJjOnJ0YXJnZXQtR2VmTVZ5aUMiLCJjOnNjaGlic3RlZC1NUVBYYXF5aCIsImM6Z3JlZW5ob3VzZS1RS2JHQmtzNCIsImM6cmVhbHplaXRnLWI2S0NreHlWIiwiYzpsZW1vbWVkaWEtemJZaHAyUWMiLCJjOnlvcm1lZGlhcy1xbkJXaFF5UyIsImM6bWF5dHJpY3NnLUFTMzVZYW05IiwiYzpzYW5vbWEiLCJjOnJhZHZlcnRpcy1TSnBhMjVIOCIsImM6cXdlcnRpemUtemRuZ0UyaHgiLCJjOmxiY2ZyYW5jZSIsImM6cmV2bGlmdGVyLWNScE1ucDV4IiwiYzpyZXNlYXJjaC1ub3ciLCJjOndoZW5ldmVybS04Vllod2IyUCIsImM6YWRtb3Rpb24iLCJjOnRoaXJkcHJlc2UtU3NLd21IVksiLCJjOmFmZmlsaW5ldCIsImM6aW50b3dvd2luLXFhenQ1dEdpIiwiYzpkaWRvbWkiLCJjOmpxdWVyeSIsImM6YWthbWFpIiwiYzphYi10YXN0eSIsImM6emFub3giLCJjOm1vYmlmeSIsImM6YXQtaW50ZXJuZXQiLCJjOnB1cnBvc2VsYS0zdzRaZktLRCIsImM6aW5mZWN0aW91cy1tZWRpYSIsImM6bWF4Y2RuLWlVTXROcWNMIiwiYzpjbG91ZGZsYXJlIiwiYzppbnRpbWF0ZS1tZXJnZXIiLCJjOmFkdmFuc2UtSDZxYmF4blEiLCJjOnJldGFyZ2V0ZXItYmVhY29uIiwiYzp0dXJibyIsImM6Y2FibGF0b2xpLW5SbVZhd3AyIiwiYzp2aWFudC00N3gyWWhmNyIsImM6dnVibGUtY01DSlZ4NGUiLCJjOmJyYW5jaC1WMmRFQlJ4SiIsImM6c2ZyLU1kcGk3a2ZOIiwiYzphcHBzZmx5ZXItWXJQZEdGNjMiLCJjOmhhc29mZmVyLThZeU1UdFhpIiwiYzpsa3FkLWNVOVFtQjZXIiwiYzpzd2F2ZW4tTFlCcmltQVoiLCJjOmZvcnR2aXNpb24taWU2YlhUdzkiLCJjOmFkaW1vLVBoVVZtNkZFIiwiYzpvc2Nhcm9jb20tRlJjaE5kbkgiLCJjOnJldGVuY3ktQ0xlclppR0wiLCJjOmlsbHVtYXRlYy1DaHRFQjRlayIsImM6YWRsaWdodG5pLXRXWkdyZWhUIiwiYzpyb2NrZXJib3gtZlRNOEVKOVAiXSwiZGlzYWJsZWQiOltdfSwicHVycG9zZXMiOnsiZW5hYmxlZCI6WyJjb29raWVzIiwic2VsZWN0X2Jhc2ljX2FkcyIsImNyZWF0ZV9hZHNfcHJvZmlsZSIsIm1lYXN1cmVfYWRfcGVyZm9ybWFuY2UiLCJtYXJrZXRfcmVzZWFyY2giLCJpbXByb3ZlX3Byb2R1Y3RzIiwic2VsZWN0X3BlcnNvbmFsaXplZF9hZHMiLCJnZW9sb2NhdGlvbl9kYXRhIiwicGVyc29ubmFsaXNhdGlvbm1hcmtldGluZyIsInByaXgiLCJtZXN1cmVhdWRpZW5jZSIsImV4cGVyaWVuY2V1dGlsaXNhdGV1ciJdLCJkaXNhYmxlZCI6W119fQ==;__Secure-InstanceId=b4ad7045-7ac5-4fab-a719-b0e745767de5;',                   
        #     'X-LBC-CC': '7',
        #     'Accept': 'application/json,application/hal+json',
        #     'User-Agent': 'LBC;Android;7.1.2;ASUS_Z01QD;phone;a3ab60b8d97f0f1a;wifi;5.69.0;569000;0',
        #     'Content-Type': 'application/json; charset=utf-8',
        #     'Host': 'api.leboncoin.fr',
        #     'Connection': 'Keep-Alive',
        #     # 'Accept-Encoding': 'gzip',
        # }
        res = await self.fetch("https://api.leboncoin.fr/finder/search",headers=self.headers,json=parameter)
        await asyncio.sleep(3)
        if self.autoSave:
            await self.saveAds(res)
        return res
        jsondata = json.dumps(parameter)
        jsondataasbytes = jsondata.encode()  
        print(self.headers)
        proxy_support = urllib.request.ProxyHandler({"https":"socks4://127.0.0.1:9050"})
        opener = urllib.request.build_opener(proxy_support)
        urllib.request.install_opener(opener)
        req = urllib.request.Request("https://api.leboncoin.fr/finder/search",headers=self.headers)
        response = urllib.request.urlopen(req, jsondataasbytes)
        # response = opener.open(req, jsondataasbytes)
        # response = requests.post(self.searchurl, headers=self.headers, verify=False,json=parameter)
        time.sleep(random.randint(5, 20))
        # print(response.status_code)
        # if response.status_code==200:
        print(response.getcode())
        if response.getcode()==200:
            resheaders = dict(response.headers._headers)
            # resheaders = response.headers
            rescookies = resheaders.get('set-cookie')
            if rescookies:
                rescookies = rescookies.split(";")
                for cookie in rescookies:
                    try:
                        key,val = cookie.split("=")
                        self.cookies[key] = val
                        self.updateCookies()
                    except:pass
            # data = response.read()
            data = gzip.decompress(response.read())
            # encoding = response.info().get_content_charset()
            # print(encoding)
            # data =data.decode("ISO-8859–1")
            # print(data)
            res= json.loads(data)
            if self.autoSave:
                self.saveAds(res)

            # res= response.json()
            return res
        else:
            print(response.status_code,"some issue do you wanna retry y/n")
            # ch = input()
            # if ch=="Y" or ch=="y":
            time.sleep(300)
            return self.CrawlLeboncoin(parameter)
            # else:
            #     return False
    def checkNext(self,res):
        try:
            next = res['pivot']
            self.parameter['pivot'] = next
            return next
        except:
            return False

    async def IntCrawling(self):
        self.autoSave = True
        res = await self.CrawlLeboncoin()
        nextpage = self.checkNext(res)
        while nextpage:
            print(nextpage)
            self.parameter['pivot'] = nextpage
            res = await self.CrawlLeboncoin()
            nextpage = self.checkNext(res)
    async def saveAds(self,res):
        ads = res.get('ads')
        if not ads:
            return 0
        # producer.PushDataList(KafkaTopicName,ads)
        # parseAds = [ParseLeboncoin(ad) for ad in ads]
        # producer.PushDataList(commonAdsTopic,parseAds)
        await producer.TriggerPushDataList(KafkaTopicName,ads)
        parseAds = [ParseLeboncoin(ad) for ad in ads]
        await producer.TriggerPushDataList(commonAdsTopic,parseAds)
        # totalads = ''
        # for ad in ads:
        #     totalads+= json.dumps(ad)+"\n"
        # if totalads:
        #     with open(f'{cpath}/{self.outputfile}.json','a') as file:
        #         file.write(totalads)
    async def UpdataAds(self):
        self.autoSave = False
        try:
            with open(f"{cpath}/lastUpdate.json","r") as file:
                lastUpdate = json.load(file)
            lastTime = self.getTimeStamp(lastUpdate['addUploadTime'])
            updated = False
        except Exception as e:
            print(e)
            print("please check your lastUpdate.json file")
            raise e 
        first = True
        while not updated:
            res = {'ads':[]}
            resdata  = await self.CrawlLeboncoin()
            # print(resdata)
            # break
            for data in resdata["ads"]:
                if first:
                    nowtime  = datetime.now()
                    nowtime = nowtime.timestamp()
                    lastupdate = {
                        "time":nowtime,
                        "source":data['url'],
                        "adid":data['list_id'],
                        "addUploadTime":data['index_date']
                    }
                    first = False
                    with open(f"{cpath}/lastUpdate.json",'w') as file:
                        file.write(json.dumps(lastupdate))
                if lastTime<self.getTimeStamp(data["index_date"]):
                    res['ads'].append(data)
                    # print("appending")
                else:
                    print("very old add",data)
                    updated = True
                    break
            await self.saveAds(res)
            nextpage = self.checkNext(resdata)
            if not nextpage:
                print("no pages left")
                break
            

    def getTimeStamp(self,strtime):
        formate = '%Y-%m-%d %H:%M:%S'
        t = datetime.strptime(strtime,formate)
        return t.timestamp()
async def updateLebonCoin():
    data = json.load(open(f'{cpath}/filter.json','r'))
    ob = LeboncoinScraper()
    try:
        await ob.init(data,"newout1")
        # ob.IntCrawling()
        await ob.UpdataAds()
    finally:ob.__del__()
async def CheckId(id):
    ob = LeboncoinScraper()
    await ob.init(proxyThread=False)
    status = await ob.CheckIfAdIdLive(id)
    ob.__del__()
    return status
async def ScrapLebonCoin():
    data = json.load(open(f'{cpath}/filter.json','r'))
    ob = LeboncoinScraper()
    try:
        await ob.init(data,"newout1")
        await ob.IntCrawling()
    finally:ob.__del__()
def leboncoinAdScraper(payload):
    typ = payload.get("real_state_type")
    if not typ:
        id = payload.get("id")
        return asyncio.run(CheckId(id))
    if typ == "Updated/Latest Ads":
        asyncio.run(updateLebonCoin())
    else:
        asyncio.run(ScrapLebonCoin())
if __name__=="__main__":
    data = json.load(open(f'{cpath}/filter.json','r'))
    ob = LeboncoinScraper(data,"newout1")
    # ob.IntCrawling()
    ob.UpdataAds()    
