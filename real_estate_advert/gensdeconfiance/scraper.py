from datetime import datetime, timedelta,timezone
from HttpRequest.requestsModules import okHTTpClient
from HttpRequest.uploader import AsyncKafkaTopicProducer
import copy
import os,json , pytz
import rstr
import random
from datetime import datetime
import time
import concurrent.futures
import os,json
import asyncio,requests
import warnings
warnings.filterwarnings("ignore", message="Unverified HTTPS request")
from saveLastChaeck import saveLastCheck
from .parser import ParseGensdeconfiance
kafkaTopicName = "gensdeconfiance_data_v1"
commonTopicName = "common-ads-data_v1"
website= "gensdeconfiance.com"
commonIdUpdate = f"activeid-{website}"
class gensdeconfiance(okHTTpClient):
    def __init__(self, proxyThread=True, proxies={}, aio=True,params={},asyncsize=1,cookies=True) -> None:
        self.apiurl = "https://gensdeconfiance.com/api/v2/ads"
        self.createurl = "https://gensdeconfiance.com/api/v2/members"
        self.loginurl = "https://gensdeconfiance.com/api/v2/members/login"
        cpath = os.path.dirname(__file__)
        URL = "https://gensdeconfiance.com/api/v2/ads?itemsPerPage=1"
        self.params = params
        proxyheaders = {}
        self.producer = AsyncKafkaTopicProducer()
        self.unchagedheaders = {
            "accept": "application/json",
            "cache-control": "no-cache",
            "appversion": "1.71.2.3200232",
            "apporigin": "android",
            "authorization": "undefined",
            "accept-language": "en-US",
            "Host": "gensdeconfiance.com",
            "Connection": "Keep-Alive",
            "Accept-Encoding": "gzip",
            "User-Agent": "okhttp/4.9.2",
            "uniquedeviceid":"96501a88a72976E6",
            "apptoken":"f9596bacf8e05070975e11df940a2084",
            "appvanity":"636e82f86000d"

        }
        super().__init__(proxyThread, URL, self.unchagedheaders, proxyheaders, proxies, aio, cpath,asyncsize,cookies)
        # self.login()
        # self.VerifyLogin()
    # async def getAdDetails(self,ad):
    #     r= await self.Asyncget(f"{self.apiurl}/{ad['uuid']}")
    #     details= {
    #             **ad,
    #             **r
    #     }
    #     return details
    def VerifyLogin(self):
        r = self.fetch(f"{self.apiurl}?itemsPerPage=1")
        if not isinstance(r,list) and r.get("code") == 403:
            try:
                os.remove(f"{self.cpath}/account.json")
                os.remove(f"{self.cpath}/session.json")
            except:pass
            self.createAccount()
            self.login()
    def login(self):
        try:
            with open(f"{self.cpath}/session.json","r") as file:
                cred = json.load(file)
                self.headers[0].update({
                    "apptoken":cred["appToken"],
                    "appvanity":cred["vanity"],
                    "uniqueDeviceId":cred["uniqueDeviceId"]
                })
        except:
            try:
                with open(f"{self.cpath}/account.json","r") as file:
                    cred = json.load(file)
                    print(cred)
                    self.headers[0]["uniquedeviceid"] = cred["uniqueDeviceId"]
                    try:
                        del self.headers[0]["apptoken"]
                        del self.headers[0]["appvanity"]
                    except:pass
                    # try:
                    body = {"email":cred["email"],"password":cred["password"],"uniqueDeviceId":cred["uniqueDeviceId"]}
                    r = self.post(self.loginurl,body=body)
                    r = {
                        **(r.json()["user"]),
                        "uniqueDeviceId":cred["uniqueDeviceId"]
                    }
                    with open(f"{self.cpath}/session.json","w") as file:
                        file.write(json.dumps(r))
            except:
                self.createAccount()
                self.login()
        # self.headers = {i:self.headers[0] for i in range(0,self.asyncsize)}
        # print(self.headers)
    def init_headers(self,sid=0,init= False):
        header = copy.deepcopy(self.unchagedheaders)
        self.session[sid].close()
        self.session[sid] = requests.Session()
        self.session[sid].verify = False
        try:
            self.proxy[sid] = (init and self.proxy.get(sid)) or self.getRandomProxy()
            # r = self.get(self.apiurl,params={"itemsPerPage":1},sid=sid)
            # cookie = r.headers.get("Set-cookie")
            cookie = self.proxy[sid]["cookies"]
            header["Cookie"] =  cookie
            self.headers[sid] = header
        except Exception as e:
            print("in test4 except", e , sid)
            self.init_headers(sid)
        pass
    def getAdDetails(self,ad,sid=0):
        r= self.fetch(f"{self.apiurl}/{ad['uuid']}",sid=sid)
        r = {
            **r,
            **ad,
        }
        # time.sleep(1)
        return r
    def splitListInNpairs(self,li,interval):
        ran = len(li)/interval
        ran = int(ran) if ran==int(ran) else int(ran)+1
        flist = []
        for  i in range(0,ran):
            item = li[interval*i:interval*(i+1)]
            flist.append(item)
        return flist
    def saveAdList(self,adsdata,onlyid=False):
        # producer.PushDataList(kafkaTopicName,adsdata)
        if onlyid:
            now = datetime.now()
            ads = [{"id":ad.get("uuid"), "last_checked": now.isoformat(),"available":True} for ad in adsdata]
            self.producer.PushDataList_v1(commonIdUpdate,ads)
        else:
            self.producer.PushDataList(kafkaTopicName,adsdata)
            parseAdList = [ParseGensdeconfiance(ad) for ad in adsdata]
            self.producer.PushDataList(commonTopicName,parseAdList)
        # final = ""
        # for da in adsdata:
        #     final+=json.dumps(da)+"\n"
        # with open(f"out.json" , "a") as file:
        #     file.write(final)
    def  save(self,data,getdata=False,onlyid=False):
        # self.initAsycSession()
        newads = []
        if onlyid:
            fetchedads = data
        else:
            fetchedads = []
            adlist = self.splitListInNpairs(data,self.asyncsize)
            for ads in adlist:
                # time.sleep(2)
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.asyncsize) as excuter:
                    # adsidlist = [ad["id"] for ad in ads]
                    futures = excuter.map(self.getAdDetails,ads,[i for i in range(0,len(ads))])
                    for f in futures:
                        fetchedads.append(f)
        # for ad in data:
        #     addetails = self.getAdDetails(ad)
        #     # tasks.append(asyncio.ensure_future(self.getAdDetails(ad)))
        #     newads.append({
        #         **ad,
        #         **addetails
        #     })
        # newads = await asyncio.gather(*tasks)
        self.saveAdList(fetchedads,onlyid=onlyid)
        # self.killAsycSession(self)
    def crawl(self,param={},first = False,onlyid=False):
        if not param:
            param = self.params
        response = self.fetch(self.apiurl, params=param)
        if first:
            return response[0]
        if not onlyid:self.createNewUpdate(latestad=response[0])
        while response: 
            self.save(response,onlyid=onlyid)
            self.params["page"]+=1
            print(f"getting {self.params['page']}")
            response = self.fetch(self.apiurl, params=self.params)
            # break
    def getLastUpdate(self):
        try:
            with open(f"{self.cpath}/lastUpdate.json",'r') as file:
                data = json.load(file)
            return data
        except:
            return {}
    def getTimeStamp(self,strtime):
        formate = '%Y-%m-%dT%H:%M:%S+'
        # 2022-11-08T10:58:38+00:00
        # 2022-06-19T05:26:55
        t = datetime.strptime(strtime,formate)
        return t.timestamp()
    def getUpdateDicFromAd(self,ad):
        nowtime  = datetime.now()
        upd={
            "timestamp": nowtime.timestamp(),
            "lastupdate": ad.get('displayDate'),
            }
        return upd
    def ranmdombirthday(self):
        """
        This function will return a random datetime between two datetime 
        objects.
        """
        start = datetime.strptime('1/1/1991 1:30 PM', '%m/%d/%Y %I:%M %p')
        end = datetime.strptime('1/1/2001 1:30 PM', '%m/%d/%Y %I:%M %p')
        delta = end - start
        int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
        random_second = random.randrange(int_delta)
        return start + timedelta(seconds=random_second)
    def createAccount(self):
        deviceid = rstr.xeger(r"[0-9]{5}a[0-9]{2}a[0-9]{5}E[0-9]{1}")
        email = rstr.xeger("[a-z]{" + f"{random.randint(3,7)}"+ "}\.[a-z]{" + f"{random.randint(1,2)}"+ "}[0-9]{" + f"{random.randint(3,7)}"+ "}@gmail\.com")   
        password = rstr.xeger(r"[a-zA-z]{4}\.[a-z]{2}[0-9]{3}") 
        fname = rstr.xeger("[a-z]{" + f"{random.randint(5,19)}"+"}")
        lname = rstr.xeger("[a-z]{" + f"{random.randint(5,19)}"+"}")
        self.headers[0]["uniquedeviceid"] = deviceid
        body ={
            "email":email,
            "locale":"en_US",
            "country":"auto",
            "uniqueDeviceId":deviceid,
            "firstName":fname,
            "lastName":lname,
            # Thu, 10 jan 2000 04:05:52 GMT
            "birthdate":datetime.strftime(self.ranmdombirthday(), "%a, %d %b %Y %H:%M:%S")+ " GMT",
            "password":password
        }
        r = self.post(self.createurl,body=body)
        d = {
            **r.json(),
            "email":email,
            "password":password,
            "uniqueDeviceId":deviceid,
        }
        with open(f"{self.cpath}/account.json","w") as file:
            file.write(json.dumps(d))
        # print(r.json())
   
    def createNewUpdate(self,latestad=None):
        if not latestad:
            latestad = self.getlatestAd()
        lastupd=self.getUpdateDicFromAd(latestad)
        with open(f"{self.cpath}/lastUpdate.json",'w') as file:
            file.write(json.dumps(lastupd))
    def getlatestAd(self):
        param = self.params
        param["orderColumn"],param["orderDirection"],param["itemsPerPage"]= "displayDate","orderDirection",1
        data = self.crawl(param=param,first=True)
        return data
    def updateLatestAd(self):
        updates = self.getLastUpdate()
        if updates:
            param = self.params
            param["orderColumn"],param["orderDirection"] = "displayDate","orderDirection"
            updated = False
            first = True
            page =1
            adcount = 0
            while not updated:
                param.update({"page":page})
                # res = self.fetch(searchurl, method = "post", json=param)
                print(param)
                ads = self.fetch(self.apiurl,params=param)
                print(ads)
                updatedads = []
                updatetimestamp = updates["lastupdate"]
                for ad in ads:
                    # ad = self.GetAdInfo(ad['id'])
                    adtimestamp = self.getUpdateDicFromAd(ad)["lastupdate"]
                    print(f"   {adtimestamp}> {updatetimestamp}====>",adtimestamp>updatetimestamp)
                    if adtimestamp>updatetimestamp:
                        if first:
                            self.createNewUpdate(ad)
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
params  = {
"category":"realestate",
"orderColumn":"displayDate",
"orderDirection":"DESC",
"type":"offering",
"rootLocales[]":"fr",
"hidden":"true",
"page":1,
"itemsPerPage":50
}
def main_scraper(payload,update=False):
    data = params
    adtype = payload.get("real_state_type")
    ob = gensdeconfiance(True,{},False,data,5)
    try:
        if adtype == "Updated/Latest Ads" or update:
            print(" latedst ads")
            ob.updateLatestAd()
        else:
            # print("pass")
            # print(ob.headers)
            # print(ob.cookie)
            ob.crawl()
    except:
        pass
    finally:
        ob.__del__()
def rescrapActiveIdbyType():
    data = params.copy()
    try:
        ob = gensdeconfiance(True,{},True,data)
        ob.crawl(onlyid=True)
    finally:
        ob.__del__()
def rescrapActiveId():
    nowtime = datetime.now()
    nowtime = nowtime - timedelta(hours=1)
    rescrapActiveIdbyType()
    # main("buy",True)
    # with concurrent.futures.ThreadPoolExecutor(max_workers=10) as excuter:
    #     futures = [excuter.submit(rescrapActiveIdbyType, i) for i in ["rental","sale"]]
    #     for f in futures:print(f)
    # rescrapActiveIdbyType("rental")
    # print("complited")
    saveLastCheck(website,nowtime.isoformat())
# try:
#     ob = gensdeconfiance(True,aio=False,params=params,asyncsize=1)
#     # ob.crawl()
#     # ob.createAccount()
# finally:
#     ob.__del__()
    