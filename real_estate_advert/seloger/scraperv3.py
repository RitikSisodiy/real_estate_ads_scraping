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
searchurl = 'https://api-seloger.svc.groupe-seloger.com/api/v1/listings/search'
ViewAddUrl = "https://api-seloger.svc.groupe-seloger.com/api/v1/listings/"
resultcounturl = "https://api-seloger.svc.groupe-seloger.com/api/v1/listings/count/"
session = requests.session()
proxy = {'https': proxyurl, 'http': proxyurl}
cpath =os.path.dirname(__file__) or "."

from HttpRequest.uploader import AsyncKafkaTopicProducer
from HttpRequest.requestsModules import HttpRequest
kafkaTopicName = "seloger_data_v1"
commonTopicName = "common-ads-data_v1"

class SelogerScraper(HttpRequest):
    token = {
            "token":"",
            "expiry":0
        }
    def __init__(self,paremeter,asyncsize=20,proxyThread=True,proxies = {}) -> None:
        cpath =os.path.dirname(__file__) or "."
        self.logfile = open(f"{cpath}/error.log",'a')
        self.timeout = 5
        self.paremeter = paremeter
        self.producer = AsyncKafkaTopicProducer()
        SELOGER_SECURITY_URL = "https://api-seloger.svc.groupe-seloger.com/api/security/register"
        headers = {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": "SeLoger/6.8.5 Dalvik/2.1.0 (Linux; U; Android 8.1.0; ASUS_X00TD Build/OPM1)",
                "Accept": "application/json",
                "Host": "api-seloger.svc.groupe-seloger.com",
                "Connection": "Keep-Alive",
                "Accept-Encoding": "gzip",
            }
        super().__init__(proxyThread, SELOGER_SECURITY_URL,{}, headers, proxies, False, cpath, asyncsize, 5)
    
    def __exit__(self):
        self.logfile.close()
    def getToken(self,sid):
        if SelogerScraper.token and SelogerScraper.token.get("expiry") and SelogerScraper.token.get("expiry") - time.time()<300:
            return SelogerScraper.token.get('token')
        headers = {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": "SeLoger/6.8.5 Dalvik/2.1.0 (Linux; U; Android 8.1.0; ASUS_X00TD Build/OPM1)",
                "Accept": "application/json",
                "Host": "api-seloger.svc.groupe-seloger.com",
                "Connection": "Keep-Alive",
                "Accept-Encoding": "gzip",
            }
        seloger_token_host = os.environ.get('HS_SELOGER_TOKEN_HOST', 'localhost')
        seloger_token_port = os.environ.get('HS_SELOGER_TOKEN_PORT', '8001')
        SELOGER_SECURITY_URL = "https://api-seloger.svc.groupe-seloger.com/api/security"
        time_token = self.session[sid].get(f"{SELOGER_SECURITY_URL}/register", headers=headers,proxies=self.proxy[sid],timeout=self.timeout).json()
        challenge_url = f"http://{seloger_token_host}:{seloger_token_port}/seloger-auth?{urllib.parse.urlencode(time_token, doseq=False)}"
        token = self.session[sid].get(challenge_url).text
        print(token,"self genrager troe")
        res = self.session[sid].get(f"{SELOGER_SECURITY_URL}/challenge",headers={**headers, **{'authorization': f'Bearer {token}'}},proxies=self.proxy[sid],timeout=self.timeout)
        assert res.status_code ==200
        final_token = res.text[1:-1]
        SelogerScraper.token["token"]  = final_token
        SelogerScraper.token["expiry"] = time.time() + 300
        return final_token
    def init_headers(self,sid=0,init=False):
        self.session[sid].close()
        self.session[sid] = requests.Session()
        try:self.proxy[sid] = (init and self.proxy.get(sid)) or self.getRandomProxy()
        except:
            self.getProxyList()
            self.init_headers()
        try:
            headers = {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": "SeLoger/6.8.5 Dalvik/2.1.0 (Linux; U; Android 8.1.0; ASUS_X00TD Build/OPM1)",
                "Accept": "application/json",
                "Host": "api-seloger.svc.groupe-seloger.com",
                "Connection": "Keep-Alive",
                "Accept-Encoding": "gzip",
            }
            final_token = self.getToken(sid)
            self.headers[sid] = {
                **headers,
                'authorization': f'Bearer {final_token}'
            }
            print(final_token,"<==========final token")
            return self.headers[sid]
        except Exception as e :
            print("excepition==============>",e)
            traceback.print_exc(file=self.logfile)
            return self.init_headers(sid=sid)
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
        page = 1
        if adtype=="sale":dic["query"]["transactionType"]=2
        else:dic["query"]["transactionType"] = 1
        totalresult =self.getTotalResult(dic)
        print(totalresult)
        acres = totalresult
        fetchedresult = 0
        try:
            with open(f"{cpath}prev{dic['query']['transactionType']}.json",'r') as file:
                iniinterval = json.load(file)
                page = iniinterval['page']
                iniinterval = iniinterval['ini']
                file = True
        except Exception as e:
            file = False
            print("==========>",e)
            iniinterval = [0,500]
        filterurllist = ''
        finalresult = 0
        maxresult = 50*200
        maxprice = self.getMaxPrize(dic)
        if not file:
            minprice = self.getMinPrize(dic)
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
                    dic.update({"pageIndex":page})
                    print(page,dic)
                    self.Crawlparam(dic,page=page)
                    page=1
                    # filterurllist += json.dumps(dic) + "/n/:"
                    iniinterval[0] = iniinterval[1]+1
                    iniinterval[1] = iniinterval[0]+int(iniinterval[0]/2)
                    finalresult +=totalresult
                elif maxresult-totalresult> 200:
                    print("elif 1")
                    last = 10
                    iniinterval[1] = iniinterval[1] + (int(iniinterval[1]/last)+1)
                elif totalresult>maxresult:
                    print("elif 2")
                    last = -10
                    iniinterval[1] = iniinterval[1] + int(iniinterval[1]/last)
                print(totalresult,"-",maxresult)
                print(iniinterval)
            self.Crawlparam(dic)
            if os.path.isfile(f'{cpath}prev{dic["query"]["transactionType"]}.json','w'):
                os.remove(f'{cpath}prev{dic["query"]["transactionType"]}.json','w')
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
        if adtype=="sale":param["query"]["transactionType"]=2
        else:param["query"]["transactionType"] = 1 
        param["query"]["sortBy"] =10
        data = self.Crawlparam(param,allPage=False,first=True)
        return data
    def getUpdateDicFromAd(self,ad):
        nowtime  = datetime.now()
        upd={
                "timestamp": nowtime.timestamp(),
                "lastupdate": self.getTimeStamp(ad.get('lastModified')),
                "Rlastupdate": ad.get('lastModified'),
                "created": self.getTimeStamp(ad.get('created')),
                "rcreated": ad.get('created'),
                "lastadId": ad.get("id"),
                "source": ad.get('permalink')
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
        t = datetime.strptime(strtime,formate)
        return t.timestamp()
    def updateLatestAd(self,adtype):
        updates = self.getLastUpdate().get(adtype)
        if updates:
            param = self.paremeter
            if adtype=="sale":param["query"]["transactionType"]=2
            else:param["query"]["transactionType"] = 1
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
                    if ad and not ad.get("created"):continue
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
    def __del__(self):
        return super().__del__()
    def save(self,adslist):
        self.producer.PushDataList(kafkaTopicName,adslist)
        parseAdList = [ParseSeloger(ad) for ad in adslist]
        self.producer.PushDataList(commonTopicName,parseAdList)
    def Crawlparam(self,param,allPage = True,first=False,save=True,page=1):
        print(param)
        if allPage:param['pageIndex'] = page
        # input()
        response = self.fetch(searchurl, method = "post", json=param,)
        if not response:
            return 0
        print(response.status_code,"+++++++++")
        res = response.json()
        pagecount = res['totalCount']
        print(pagecount)
        ads = res['items']
        totalpage = pagecount/len(ads)
        totalpage = int(totalpage) if int(totalpage)==totalpage else int(totalpage)+1
        print(f"total page {totalpage}")
        # input()
        print(len(ads),"_total ads+++++++++++++")
        if first:
            ads = ads[:1]
        try:
            data = {
                    "ini":[param["query"]['minimumPrice'],param["query"]['maximumPrice']],
                    "page": param["pageIndex"]
                    }
            with open(f'{cpath}prev{param["query"]["transactionType"]}.json','w') as file:
                    file.write(json.dumps(data))
        except Exception as e:
            print("exception=========>",e) 
            pass
        adlist = self.splitListInNpairs(ads,self.asyncsize)
        fetchedads = []
        for ads in adlist:
            # time.sleep(2)
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.asyncsize) as excuter:
                adsidlist = [ad["id"] for ad in ads]
                futures = excuter.map(self.GetAdInfo,adsidlist,[i for i in range(0,len(adsidlist))])
                for f in futures:
                    fetchedads.append(f)
                # excuter.shutdown(wait=True)
                # adInfo = self.GetAdInfo(ad["id"])
                # adlist.append(adInfo)
            # with open("sampleout4.json",'a') as file:
            #     file.write(json.dumps(adInfo)+"\n")
        if first:
            return fetchedads[0]
        if save:self.save(fetchedads)
        if allPage:
            totalpage = 200 if totalpage>200 else totalpage
            for i in range(int(param["pageIndex"])+1,totalpage+1):
                param["pageIndex"] = i
                self.Crawlparam(param,allPage=False)
        else:
            return fetchedads
    def CrawlSeloger(self,adtype):
        self.createNewUpdate(adtype,latestad=None)
        filterlist= self.genFilter(adtype)
        # for Filter in filterlist:
        #     self.Crawlparam(Filter)
def CheckId(id):
    ob = SelogerScraper({},asyncsize=1,proxyThread=False,proxies=[{"https":"http://sp30786500:Legals786@eu.dc.smartproxy.com:20000/"}])
    r = ob.fetch(f"{ViewAddUrl}{id}")
    if r.status_code ==404:
        return False
    if r.status_code == 200:
        return True
    return False
def main_scraper(payload,update=False):
    data = json.load(open(f"{cpath}/selogerapifilter.json",'r'))
    try:
        adtype = payload.get("real_state_type")
        if adtype == "Updated/Latest Ads" or update:
            ob = SelogerScraper(data,asyncsize=5)
            print(" latedst ads")
            ob.updateLatestAd("rental")
            ob.updateLatestAd("sale")
        else:
            ob = SelogerScraper(data,asyncsize=10)
            ob.CrawlSeloger(adtype)
    finally:
        ob.__del__()