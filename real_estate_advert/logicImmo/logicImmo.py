import concurrent.futures
from datetime import datetime
import imp
from fastapi import FastAPI
from pytest import param
import threading
import requests
import json
import random
import urllib
import os
from .scrapProxy import ProxyScraper
from .parser import ParseSeloger
import traceback
proxyurl = "http://lum-customer-c_5afd76d0-zone-residential:7nuh5ts3gu7z@zproxy.lum-superproxy.io:22225"
import time
searchurl = 'https://api-logicimmo.svc.groupe-seloger.com/api/v3/listings/search'
ViewAddUrl = "https://api-logicimmo.svc.groupe-seloger.com/api/v3/listings/"
resultcounturl = "https://api-logicimmo.svc.groupe-seloger.com/api/v1/listings/count/"
session = requests.session()
proxy = {'https': proxyurl, 'http': proxyurl}

try:
    from uploader import AsyncKafkaTopicProducer
except:
    from .uploader import AsyncKafkaTopicProducer
cpath =os.path.dirname(__file__)
kafkaTopicName = "logicImmo_data_v1"
commonTopicName = "common-ads-data_v1"
class SelogerScraper:
    def __init__(self,paremeter,asyncsize=20) -> None:
        self.logfile = open(f"{cpath}/error.log",'a')
        self.timeout = 5
        
        
        SELOGER_SECURITY_URL = "https://api-logicimmo.svc.groupe-seloger.com/api/security/register"
        headers = {
                    'User-Agent': 'okhttp/4.6.0',
                }
        self.prox = ProxyScraper(SELOGER_SECURITY_URL,headers)
        self.paremeter= paremeter
        try:
            self.proxies = self.readProxy()
            if not self.proxies:
                self.getProxyList()
        except:
            self.getProxyList()
        self.proxyUpdateThread()
        self.asyncsize=asyncsize
        self.headers = {}
        self.proxy = {}
        self.session = {i:requests.Session() for i in range(0,asyncsize)}
        self.headers = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.asyncsize) as excuter:
            futures = excuter.map(self.init_headers,[i for i in range(0,asyncsize)])
            count = 0
            for f in futures:
                self.headers[count] = f
                count+=1
        self.producer = AsyncKafkaTopicProducer()
    def getProxyList(self):
        self.prox.FetchNGetProxy()
        self.prox.save(cpath)
        self.proxies = self.readProxy()
    def updateProxyList(self,interval=300):
        if self.readProxy():time.sleep(interval)
        start = True
        while start:
            self.getProxyList()
            time.sleep(interval) 
            start = self.startThread 
    def proxyUpdateThread(self):
        print("proxy thread is started")
        self.startThread = True
        self.proc = threading.Thread(target=self.updateProxyList, args=())
        self.proc.daemon = True
        self.proc.start()
    
    def __del__(self):
        self.startThread = False
        print("proxy thread is terminated")
    def readProxy(self):
        with open(f"{cpath}/working.txt","r") as file:
            proxies = file.readlines()
        return [json.loads(proxy) for proxy in proxies]
    def getRandomProxy(self):
        proxy = random.choice(self.proxies)
        return proxy
    def __exit__(self):
        self.logfile.close()
    def init_headers(self,sid=0):
        self.session[sid].close()
        self.session[sid] = requests.Session()
        try:self.proxy[sid] = self.getRandomProxy()
        except:
            self.getProxyList()
            self.init_headers()
        try:
            final_token = self.getToken(sid)
            self.headers[sid] = {
                'accept': 'application/json',
                'user-agent': 'Mobile;Android;SeLoger;6.4.2',
                'authorization': f'Bearer {final_token}',
                'content-type': 'application/json; charset=utf-8'
            }
            print(final_token,"<==========final token")
            return self.headers[sid]
        except Exception as e :
            print("excepition==============>",e)
            traceback.print_exc(file=self.logfile)
            return self.init_headers(sid=sid)
    def getToken(self,sid):
        headers = {
                'user-agent': 'okhttp/4.6.0',
                'User-Agent': 'okhttp/4.6.0',
            }
        seloger_token_host = os.environ.get('HS_SELOGER_TOKEN_HOST', 'localhost')
        seloger_token_port = os.environ.get('HS_SELOGER_TOKEN_PORT', '8001')

        SELOGER_SECURITY_URL = "https://api-logicimmo.svc.groupe-seloger.com/api/security"
        time_token = self.session[sid].get(f"{SELOGER_SECURITY_URL}/register", headers=headers,proxies=self.proxy[sid],timeout=self.timeout).json()
        challenge_url = f"http://{seloger_token_host}:{seloger_token_port}/seloger-auth?{urllib.parse.urlencode(time_token, doseq=False)}"
        token = self.session[sid].get(challenge_url).text
        print(token,"self genrager troe")
        final_token = self.session[sid].get(f"{SELOGER_SECURITY_URL}/challenge",headers={**headers, **{'authorization': f'Bearer {token}'}},proxies=self.proxy[sid],timeout=self.timeout).text[1:-1]
        return final_token
    def fetch(self,url,method = "get",sid=0,retry=0,**kwargs):
        kwargs['headers'] = self.headers[sid]
        kwargs['proxies'] = self.proxy[sid]
        try:
            if method=="post":
                r = self.session[sid].post(url,timeout=self.timeout,**kwargs)
            else:
                r = self.session[sid].get(url,timeout=self.timeout,**kwargs)
            print(f"{r.status_code} : response status")
        except Exception as e:
            traceback.print_exc(file=self.logfile)
            print(e)
            time.sleep(1)
            self.init_headers(sid=sid)
            if retry<10:
                retry+=1
                return self.fetch(url,method=method,sid=sid,retry=retry,**kwargs)
            else:return None
        if r.status_code!=200:
            print(url)
            print(method)
            print(kwargs)
            self.init_headers(sid=sid)
            if retry<10:
                retry+=1
                return self.fetch(url,method=method,sid=sid,retry=retry,**kwargs)
            else:return None
        return r
    def GetAdInfo(self,addId:int,sid=0):
        url = f"{ViewAddUrl}{addId}"
        response = self.fetch(url,sid=sid,proxies=self.proxy[sid])
        try:return response.json()
        except:{}
    def getTotalResult(self,param,sid=0):
        # print(param)
        param = [param['query']]
        url = resultcounturl
        r = self.fetch(url,method="post",sid=sid,json=param,proxies=self.proxy[sid])
        try:
            count = r.json()[0]
        except:
            count = 0
        if count == -1:return 0
        else: return count
    
    def getMaxPrize(self,param,sid=0):
        # sorting values "sortBy"
        # 1 - prize INCREASING order
        # 2 - prize DECREASING order
        # 10 - DATE NEWEST FIRST
        # 9 - ate OLDEST FIRST
        # 5 - GROWING SURFACE
        # 6 - DECREASING SURFACE
        param["query"]["sortBy"] =2
        # print(param)
        try:
            prize = self.fetch(searchurl,method="post",sid=sid,json=param).json()["items"][0]['price']
            prize = str(prize)
        except:
            prize = "1000"
        maxprice = ''
        for c in prize:
            try:maxprice+=f"{int(c)}"
            except:pass
        if maxprice:return int(maxprice)+1
    def getMinPrize(self,param,sid=0):
        # sorting values "sortBy"
        # 1 - prize INCREASING order
        # 2 - prize DECREASING order
        # 10 - DATE NEWEST FIRST
        # 9 - ate OLDEST FIRST
        # 5 - GROWING SURFACE
        # 6 - DECREASING SURFACE
        param["query"]["sortBy"] =1
        # print(param)
        try:
            prize = self.fetch(searchurl,method="post",sid=sid,json=param).json()["items"][0]['price']
            prize = str(prize)
        except:
            prize = "1000"
        maxprice = ''
        for c in prize:
            try:maxprice+=f"{int(c)}"
            except:pass
        if maxprice:return int(maxprice)
        else: return 0
    def splitListInNpairs(self,li,interval):
        ran = len(li)/interval
        ran = int(ran) if ran==int(ran) else int(ran)+1
        flist = []
        for  i in range(0,ran):
            item = li[interval*i:interval*(i+1)]
            flist.append(item)
        return flist
    def genFilter(self,adtype):
        dic = self.paremeter
        if adtype=="sale":dic["listingSearchCriterias"]["transactionTypesIds"]=[3]
        else:dic["listingSearchCriterias"]["transactionTypesIds"] = 1
        totalresult =self.getTotalResult(dic)
        print(totalresult)
        acres = totalresult
        fetchedresult = 0
        iniinterval = [0,500]
        filterurllist = ''
        finalresult = 0
        maxresult = 50*200
        maxprice = self.getMaxPrize(dic)
        minprice = self.getMinPrize(dic)
        if minprice:
            iniinterval[0]=minprice
            iniinterval[1]=minprice+1
        print(maxprice)
        print(totalresult>=maxresult)
        if totalresult>=maxresult:
            while iniinterval[1]<=maxprice:
                dic["query"]['minimumPrice'],dic["query"]['maximumPrice'] = iniinterval
                totalresult = self.getTotalResult(dic)
                if totalresult < maxresult and maxresult-totalresult<=3000:
                    print(f"condition is stisfy going to next interval {totalresult}")
                    last = 1
                    dic.update({"pageIndex":1})
                    self.Crawlparam(dic)
                    # filterurllist += json.dumps(dic) + "/n/:"
                    iniinterval[0] = iniinterval[1]+1
                    iniinterval[1] = iniinterval[0]+int(iniinterval[0]/2)
                    finalresult +=totalresult
                elif maxresult-totalresult> 200:
                    print("elif 1")
                    last = 10
                    iniinterval[1] = iniinterval[1] + int(iniinterval[1]/last)
                elif totalresult>maxresult:
                    print("elif 2")
                    last = -10
                    iniinterval[1] = iniinterval[1] + int(iniinterval[1]/last)
                print(totalresult,"-",maxresult)
                print(iniinterval)
            self.Crawlparam(dic)
            # filterurllist+=json.dumps(dic)
            finalresult +=totalresult
        print(f"{finalresult},{acres}")
        # filterurllist = [json.loads(query) for query in filterurllist.split("/n/:")]
        # return filterurllist
    def getLastUpdate(self):
        try:
            with open(f"{cpath}/lastUpdate.json",'r') as file:
                data = json.load(file)
            return data
        except:
            return {}
    def getlatestAd(self,adtype):
        param = self.paremeter
        if adtype=="sale":param["listingSearchCriterias"]["transactionTypesIds"]=[1]
        else:param["listingSearchCriterias"]["transactionTypesIds"] = [2] 
        param["searchParameters"]["sortBy"] =1
        size = param["searchParameters"]["limit"]
        param["searchParameters"]["limit"] =1
        data = self.Crawlparam(param,allPage=False,first=True)
        param["searchParameters"]["limit"] = size
        return data
    def getUpdateDicFromAd(self,ad):
        nowtime  = datetime.now()
        upd={
                "timestamp": nowtime.timestamp(),
                "lastupdate": self.getTimeStamp(ad.get('updateDate')),
                "Rlastupdate": ad.get('updateDate'),
                "created": self.getTimeStamp(ad.get('firstOnlineDate')),
                "rcreated": ad.get('firstOnlineDate'),
                "lastadId": ad.get("propertyId"),
                "source": ad.get('url')
            }
        return upd
    def createNewUpdate(self,adtype,latestad=None):
        lastupd = self.getLastUpdate()
        if not latestad:
            latestad = self.getlatestAd(adtype)
        lastupd[adtype]=self.getUpdateDicFromAd(latestad)
        with open(f"{cpath}/lastUpdate.json",'w') as file:
            file.write(json.dumps(lastupd))
    def getTimeStamp(self,strtime):
        formate = '%Y-%m-%dT%H:%M:%S'
        # 2022-06-19T05:26:55
        try:
            t = datetime.strptime(strtime,formate)
        except:return strtime
        return t.timestamp()
    def updateLatestAd(self,adtype):
        updates = self.getLastUpdate().get(adtype)
        if updates:
            param = self.paremeter
            if adtype=="sale":param["query"]["transactionType"]=[1]
            else:param["query"]["transactionType"] = [2]
            param["query"]["sortBy"]= 10
            updated = False
            first = True
            page =1
            adcount = 0
            while not updated and page<=201:
                param.update({"pageIndex":page})
                # res = self.fetch(searchurl, method = "post", json=param)
                ads = self.Crawlparam(param,False,False,False)
                updatedads = []
                updatetimestamp = updates["created"]
                for ad in ads:
                    # ad = self.GetAdInfo(ad['id'])
                    adtimestamp = self.getTimeStamp(ad["created"])
                    print(f"   {adtimestamp}> {updatetimestamp}====>",adtimestamp>updatetimestamp)
                    if adtimestamp>updatetimestamp:
                        if first:
                            self.createNewUpdate(adtype,ad)
                            first=False
                        updatedads.append(ad)
                        adcount+=1
                    else:
                        print(f"{adcount} new ads scraped ")
                        updated = True
                        break
                self.save(updatedads)
                page+=1
        else:
            print("there is no update available l")
    def save(self,adslist):
        self.producer.PushDataList(kafkaTopicName,adslist)
        # parseAdList = [ParseSeloger(ad) for ad in adslist]
        # self.producer.PushDataList(commonTopicName,parseAdList)
    def Crawlparam(self,param,allPage = True,first=False,save=True):
        print(param)
        if allPage:param["searchParameters"]["offset"] = 1
        # input()
        response = self.fetch(searchurl, method = "post", json=param,)
        if not response:
            return 0
        print(response.status_code,"+++++++++")
        res = response.json()
        pagination = res['pagination']
        pagesize = pagination["pageSize"]
        ads = res['items']
        totalpage = pagination["totalCount"]
        print(f"total page {totalpage}")
        # input()
        print(len(ads),"_total ads+++++++++++++")
        if first:
            ads = ads[:1]
        fetchedads = ads
        if first:
            return fetchedads[0]
        if save:self.save(fetchedads)
        if allPage:
            for i in range(1,totalpage):
                param["searchParameters"]["offset"] = (i*pagesize) or 1
                self.Crawlparam(param,allPage=False)
        else:
            return fetchedads
    def CrawlSeloger(self,adtype):
        param = self.paremeter
        self.createNewUpdate(adtype,latestad=None)
        self.Crawlparam(param)
        # filterlist= self.genFilter(adtype)
        # for Filter in filterlist:
        #     self.Crawlparam(Filter)
def main_scraper(payload,update=False):
    data = json.load(open(f"{cpath}/selogerapifilter.json",'r'))
    adtype = payload.get("real_state_type")
    if adtype == "Updated/Latest Ads" or update:
        ob = SelogerScraper(data,asyncsize=5)
        print("updateing latedst ads")
        ob.updateLatestAd("rental")
        ob.updateLatestAd("sale")
    else:
        ob = SelogerScraper(data,asyncsize=1)
        ob.CrawlSeloger(adtype)
    ob.__del__()