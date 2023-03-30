from datetime import datetime,timedelta
import requests
import json
import urllib,settings
import os,concurrent.futures

from saveLastChaeck import saveLastCheck
# from .scrapProxy import ProxyScraper

from .parser import ParseLogicImmo
import traceback
proxyurl = "http://lum-customer-c_5afd76d0-zone-residential:7nuh5ts3gu7z@zproxy.lum-superproxy.io:22225"
import time
searchurl = 'https://api-logicimmo.svc.groupe-seloger.com/api/v3/listings/search'
ViewAddUrl = "https://api-logicimmo.svc.groupe-seloger.com/api/v3/listings/"
resultcounturl = "https://api-logicimmo.svc.groupe-seloger.com/api/v1/listings/count/"
session = requests.session()
proxy = {'https': proxyurl, 'http': proxyurl}
from HttpRequest.uploader import AsyncKafkaTopicProducer
from HttpRequest.requestsModules import HttpRequest
cpath =os.path.dirname(__file__) or "."
kafkaTopicName = settings.KAFKA_LOGICIMMO
commonTopicName = settings.KAFKA_COMMON_PATTERN
website= "logic-immo.com"
commonIdUpdate = f"activeid-{website}"
class LogicImmoScraper(HttpRequest):
    token = {
            "token":"",
            "expiry":0
        }
    def __init__(self,paremeter,asyncsize=20,timeout = 5,proxy = None,maxtry=False) -> None:
        cpath =os.path.dirname(__file__) or "."
        self.logfile = open(f"{cpath}/error.log",'a')
        self.timeout = timeout
        SELOGER_SECURITY_URL = "https://api-logicimmo.svc.groupe-seloger.com/api/security/register"
        headers = {
                    'User-Agent': 'okhttp/4.6.0',
                }
        self.maxtry = maxtry
        self.producer = AsyncKafkaTopicProducer()
        self.paremeter= paremeter
        super().__init__(True, SELOGER_SECURITY_URL,{}, headers, {}, False, cpath, asyncsize, 5)
    def __del__(self):
        return super().__del__()
    def __exit__(self):
        self.logfile.close()
    def init_headers(self,sid=0,init= False):
        self.session[sid].close()
        self.session[sid] = requests.Session()
        try:self.proxy[sid] = (init and self.proxy.get(sid)) or self.getRandomProxy()
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
        if LogicImmoScraper.token and LogicImmoScraper.token.get("expiry") and LogicImmoScraper.token.get("expiry") - time.time()<300:
            return LogicImmoScraper.token.get('token')
        headers = {
                'user-agent': 'okhttp/4.6.0',
                'User-Agent': 'okhttp/4.6.0',
            }
        seloger_token_host = os.environ.get('HS_SELOGER_TOKEN_HOST', 'localhost')
        seloger_token_port = os.environ.get('HS_SELOGER_TOKEN_PORT', '8001')

        SELOGER_SECURITY_URL = "https://api-logicimmo.svc.groupe-seloger.com/api/security"
        time_token = self.session[sid].get(f"{SELOGER_SECURITY_URL}/register", headers=headers,proxies=self.proxy[sid],timeout=5).json()
        challenge_url = f"http://{seloger_token_host}:{seloger_token_port}/seloger-auth?{urllib.parse.urlencode(time_token, doseq=False)}"
        token = self.session[sid].get(challenge_url).text
        print(token,"self genrager troe")
        res = self.session[sid].get(f"{SELOGER_SECURITY_URL}/challenge",headers={**headers, **{'authorization': f'Bearer {token}'}},proxies=self.proxy[sid],timeout=self.timeout)
        assert res.status_code == 200
        final_token = res.text[1:-1]
        LogicImmoScraper.token["token"]  = final_token
        LogicImmoScraper.token["expiry"] = time.time() + 300
        return final_token
    def fetch(self,url,method = "get",sid=0,retry=0,**kwargs):
        kwargs['headers'] = self.headers[sid]
        kwargs['proxies'] = self.proxy[sid]
        kwargs["timeout"] = kwargs.get("timeout") or self.timeout
        try:
            if method=="post":
                r = self.session[sid].post(url,**kwargs)
            else:
                r = self.session[sid].get(url,**kwargs)
            print(f"{r.status_code} : response status")
        except Exception as e:
            traceback.print_exc(file=self.logfile)
            print(e)
            time.sleep(1)
            self.init_headers(sid=sid)
            if retry<10:
                if not self.maxtry:retry+=1
                return self.fetch(url,method=method,sid=sid,retry=retry,**kwargs)
            else:return None
        if r.status_code not in [200,404]:
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
        if adtype=="sale":dic["listingSearchCriterias"]["transactionTypesIds"]=[1,4,5,11]
        else:dic["listingSearchCriterias"]["transactionTypesIds"] = [2,3]
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
        if adtype=="sale":param["listingSearchCriterias"]["transactionTypesIds"]=[1,4,5,11]
        else:param["listingSearchCriterias"]["transactionTypesIds"] = [2,3] 
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
    def getAdStatus(self,id):
        r = self.fetch(f"{ViewAddUrl}{id}","get")
        print(r)
        return r.status_code
    def updateLatestAd(self,adtype):
        updates = self.getLastUpdate().get(adtype)
        res = []
        if updates:
            param = self.paremeter
            if adtype=="sale":param["listingSearchCriterias"]["transactionTypesIds"]=[1,4,5,11]
            else:param["listingSearchCriterias"]["transactionTypesIds"] = [2,3] 
            param["searchParameters"]["sortBy"] =1
            self.paremeter["searchParameters"]["limit"] = 100
            updated = False
            first = True
            page =1
            adcount = 0
            page = 0
            pagesize = self.paremeter["searchParameters"]["limit"]
            while not updated:
                param["searchParameters"]["offset"] = (page*pagesize) or 1  
                param.update({"pageIndex":page})
                # res = self.fetch(searchurl, method = "post", json=param)
                ads = self.Crawlparam(param,False,False,False)
                updatedads = []
                updatetimestamp = updates["lastupdate"]
                for ad in ads:
                    # ad = self.GetAdInfo(ad['id'])
                    adtimestamp = ad["updateDate"]
                    print(f"   {adtimestamp}> {updatetimestamp}====>",adtimestamp>updatetimestamp)
                    if adtimestamp>updatetimestamp:
                        if first:
                            self.createNewUpdate(adtype,ad)
                            first=False
                        updatedads.append(ad)
                        adcount+=1
                    else:
                        res = f"{adcount} new {adtype} ads scraped "
                        print(f"{adcount} new ads scraped ")
                        updated = True
                        break
                self.save(updatedads)
                page+=1
        else:
            res = f"there is no update available  {adtype}"
            print(res)
        return res
    def save(self,adslist,onlyid=False):
        if onlyid:
            now = datetime.now()
            ads = [{"id":ad.get("id"), "last_checked": now.isoformat(),"available":True} for ad in adslist]
            self.producer.PushDataList_v1(commonIdUpdate,ads)
        else:
            self.producer.PushDataList(kafkaTopicName,adslist)
            parseAdList = [ParseLogicImmo(ad) for ad in adslist]
            self.producer.PushDataList(commonTopicName,parseAdList)

    def Crawlparam(self,param,allPage = True,first=False,save=True,onlyid=False):
        print(param)
        if allPage:param["searchParameters"]["offset"] = 0
        # input()
        response = self.fetch(searchurl, method = "post", json=param,)
        if not response:
            return 0
        print(response.status_code,"+++++++++")
        res = response.json()
        pagination = res['pagination']
        pagesize = pagination["pageSize"]
        ads = res['items']
        if save:self.save(ads,onlyid=onlyid)
        if not allPage:
            return ads
        totalpage = pagination["totalCount"]
        print(f"total page {totalpage}")
        # input()
        print(len(ads),"_total ads+++++++++++++")
        if first:
            ads = ads[:1]
        fetchedads = ads
        if pagesize:
            totalpage = int(totalpage/pagesize)+1
        if first:
            return fetchedads[0]
        if allPage:
            while True:
                param["searchParameters"]["offset"] += len(ads)
                ads = self.Crawlparam(param,allPage=False,onlyid=onlyid)
                if not ads:break
    def CrawlSeloger(self,adtype,onlyid=False):
        param = self.paremeter.copy()
        if adtype=="sale":param["listingSearchCriterias"]["transactionTypesIds"]=[1,4,5,11]
        else:param["listingSearchCriterias"]["transactionTypesIds"] = [2,3] 
        if not onlyid :self.createNewUpdate(adtype,latestad=None)
        self.Crawlparam(param,onlyid=onlyid)
        # filterlist= self.genFilter(adtype)
        # for Filter in filterlist:
        #     self.Crawlparam(Filter)
data = json.load(open(f"{cpath}/selogerapifilter.json",'r'))
def CheckId(id):
    ob = LogicImmoScraper(data,asyncsize=1,proxy={"https":"http://sp30786500:Legals786@eu.dc.smartproxy.com:20000/"})
    r= ob.getAdStatus(id)
    ob.__del__()
    if r==200:found = True
    else:found = False
    return found
def rescrapActiveIdbyType():
    try:
        readdata = json.load(open(f"{cpath}/selogerapifilter.json",'r'))
        ob = LogicImmoScraper(readdata.copy(),asyncsize=1,timeout=10,maxtry=True)
        ob.CrawlSeloger("rental",onlyid=True)
    finally:
        ob.__del__()
    try:
        readdata = json.load(open(f"{cpath}/selogerapifilter.json",'r'))
        ob = LogicImmoScraper(readdata.copy(),asyncsize=1,timeout=10,maxtry=True)
        ob.CrawlSeloger("sale",onlyid=True)
    finally:
        ob.__del__()
def rescrapActiveId():
    nowtime = datetime.now()
    nowtime = nowtime - timedelta(hours=1)
    # main("buy",True)
    # with concurrent.futures.ThreadPoolExecutor(max_workers=10) as excuter:
    #     futures = [excuter.submit(rescrapActiveIdbyType, i) for i in ["rental","sale"]]
    #     for f in futures:print(f)
    # rescrapActiveIdbyType("rental")
    rescrapActiveIdbyType()
    # print("complited")
    saveLastCheck(website,nowtime.isoformat())
def main_scraper(payload,update=False):
    try:
        adtype = payload.get("real_state_type")
        if adtype == "Updated/Latest Ads" or update:
            data["searchParameters"]["limit"] = 100
            ob = LogicImmoScraper(data,asyncsize=1,timeout=10)
            print("updateing latedst ads")
            return str(ob.updateLatestAd("rental")) +","+ str(ob.updateLatestAd("sale"))
        elif payload.get("real_state_type") == "deletedCheck":
            rescrapActiveId()
        else:
            ob = LogicImmoScraper(data,asyncsize=1,timeout=30)
            ob.CrawlSeloger(adtype)
    finally:
        try:ob.__del__()
        except:pass