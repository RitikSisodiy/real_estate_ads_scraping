
import random
from tabnanny import check
import threading
import requests
from xml.dom import ValidationErr
from seleniumwire import webdriver
from selenium.webdriver.chrome.options import Options
from getChrome import getChromePath
from urllib.parse import urlencode
import json,os,time
from  .parser import ParsePap
from datetime import datetime
# from .formater import formater
# try:from scrapProxy import ProxyScraper
from HttpRequest.AsyncProxy import ProxyScraper
# except:from .scrapProxy import ProxyScraper
from HttpRequest.uploader import AsyncKafkaTopicProducer
from kafka_publisher import KafkaTopicProducer
# producer = KafkaTopicProducer()
producer = AsyncKafkaTopicProducer()
kafkaTopicName = "pap_data_v1"
commanPattern ="common-ads-data_v1"
cpath =os.path.dirname(__file__) or "."
chrome = getChromePath()
class PapScraper:
    def __init__(self,parameter,proxy=None) -> None:
        self.parameter = parameter
        self.apiurl = "https://api.pap.fr/app/annonces"
        self.proxy = proxy
        SELOGER_SECURITY_URL = "https://www.google.com"
        
        self.cookie = ""
        headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36',
                }
        self.prox = ProxyScraper(SELOGER_SECURITY_URL,headers)
        try:
            self.proxies = self.readProxy()
        except:
            self.getProxyList()
        self.proxyUpdateThread()
        # self.sesson  = self.getDriver()
        self.session = requests.session()
        self.proxy = self.getRandomProxy()
        # self.driver.proxy = self.getRandomProxy()
        # self.driver.set_page_load_timeout(5)
        self.headers = {
            "Accept": "*/*",
            "X-App-Version": "4.0.10",
            "X-App-Target": "android",
            "X-App-Uuid": "b8c75b38-b638-4269-acaf-722f42936bec",
            "User-Agent": "PAP/G-4.0.10 (Google sdk_gphone_x86 Android SDK 30) okhttp/5.0.0-alpha.2",
            "Host": "api.pap.fr",
            "Connection": "Keep-Alive",
            "Accept-Encoding": "gzip",
            }
        # self.driver.request_interceptor = self.interceptor
        pass
    def GenCookie(self,proxy=None):
        import subprocess
        if proxy:proc = subprocess.check_output(f'java -jar "{cpath}/scrap.jar" {proxy["http"]} 10' ,stderr=subprocess.STDOUT,shell=True)
        else:proc = subprocess.check_output(f'java -jar "{cpath}/scrap.jar"' ,stderr=subprocess.STDOUT,shell=True)
        d = proc.decode('UTF-8')
        print(d)
        if "error" not in d:
            return d.strip()
        else:return False
    def getRandomProxy(self):
        proxy = random.choice(self.proxies)
        return proxy
    def proxyUpdateThread(self):
        print("proxy thread is started")
        self.startThread = True
        self.proc = threading.Thread(target=self.updateProxyList, args=())
        self.proc.daemon = True
        self.proc.start()
    def threadsleep(self,t):
        while(t>0):
            if not self.startThread:
                return True
            time.sleep(1)
            t-=1
        return False
    def updateProxyList(self,interval=300):
        if self.readProxy():time.sleep(interval)
        while self.startThread:
            self.cookie = self.GenCookie()
            self.getProxyList()
            b = self.threadsleep(interval)
            if b:
                break
        print("thread is stopped")
    def getProxyList(self):
        self.prox.FetchNGetProxy()
        self.prox.save(cpath)
        self.proxies = self.readProxy()
    def readProxy(self):
        with open(f"{cpath}/working.txt","r") as file:
            proxies = file.readlines()
        return [json.loads(proxy) for proxy in proxies]
    def interceptor(self,request):
        for key,val in self.headers.items():
            try:del requests.headers[key] # Delete the header first
            except:pass
            requests.headers[key] = val
    def getDriver(self):
        options = webdriver.ChromeOptions()
        # options.add_argument('--proxy-server=%s' % "socks4://127.0.0.1:9050")
        options.add_argument("--headless")
        # proxy = "http://lum-customer-c_5afd76d0-zone-data_center:r33r92fcpqmz@zproxy.lum-superproxy.io:22225"
        driver = webdriver.Chrome(chrome, chrome_options=options)
        # time.sleep(10)
        return driver

    def getTotalResult(self,resp):
        # print(resp)
        try:
            ads = resp.get("annonces")
            if ads:
                return len(ads)
            else:
                return 0
        except:
            return 0
    def fetchJson(self,url,params=None):
        if params:
            qury = urlencode(params)
            url = f"{url}?{qury}"
        try:
            if self.cookie:self.headers["cookie"] = self.cookie
            print(self.proxy)
            r= self.session.get(url,headers=self.headers,proxies=self.proxy,timeout=5)
            print(r)
            # self.driver.get(url)
            # content = self.driver.page_source
            # content = self.driver.find_element_by_tag_name('pre').text
            if r.status_code==200:
                parsed_json = r.json()
            elif r.status_code == 403:
                cookie = self.GenCookie(self.proxy)
                if not cookie: raise ValidationErr
                else:self.cookie=cookie
                return self.fetchJson(url,params)
            else:
                raise ValidationErr
        except Exception as e:
            print("exeptin", self.cookie)
            self.proxy = self.getRandomProxy()
            self.session.close()
            self.session = requests.session()
            return self.fetchJson(url)
        return parsed_json
    def save(self,data,getdata=False):
        ads = data["annonces"]
        # tasks = [ad['_links']['self']['href'] for ad in ads]
        adlist = []
        for ad in ads:
            adid = ad["id"]
            adurl = f"{self.apiurl}/detail?id={adid}"
            adinfo = self.fetchJson(adurl)
            adlist.append(adinfo)
        self.saveAdList(adlist)
    def getLastUpdate(self):
        try:
            with open(f"{cpath}/lastUpdate.json",'r') as file:
                data = json.load(file)
            return data
        except:
            return {}
    def createNewUpdate(self,typ,latestad):
        lastupd = self.getLastUpdate()
        nowtime  = datetime.now()
        latestad = latestad['annonce']
        lastupd[typ]={
                "timestamp": nowtime.timestamp(),
                # "lastupdate": latestad['date_classement'],
                "lastadId": latestad.get("id"),
                "source": latestad["url"]
            }
        with open(f"{cpath}/lastUpdate.json",'w') as file:
            file.write(json.dumps(lastupd))
    def GetLatestad(self,param):
        latestad = self.fetchJson(self.apiurl,params=param)
        latestadid = latestad.get("annonces")[0]["id"]
        ad = self.fetchJson(f"{self.apiurl}/detail?id={latestadid}")
        return ad
    def CrawlLatest(self,typ):
        dic = self.parameter
        lastupdates = self.getLastUpdate()[typ]
        dic["recherche[produit]"] = typ
        response = self.fetchJson(self.apiurl,params=dic)
        adsdata = response['_embedded']["annonce"]
        finalads = []
        latad = adsdata[0]
        first = True
        for sads in adsdata:
            adurl = sads['_links']['self']['href']
            ads = self.fetchJson(adurl)
            if first:
                latad = ads
                first=False
            print(ads['date_classement'],int(lastupdates["lastupdate"]))
            if ads['date_classement']<=int(lastupdates["lastupdate"]):
                break
            else:
                finalads.append(ads)
            # if latad['date_classement']< ads['date_classement']:
            #     latad = ads
        self.createNewUpdate(typ,latad)
        print(f"{len(finalads)} new ads scraped")
        self.saveAdList(finalads)
    def CrawlLatestV2(self,typ):
        param = self.parameter
        param.update({"produit":typ})
        response = self.fetchJson(self.apiurl, params=param)
        self.save(response)
    def saveAdList(self,adsdata):
            # for data in adsdata:
            #     producer.kafka_producer_sync(kafkaTopicName,data)
            adlist = []
            for ad in adsdata:
                ad = ad.get("annonce")
                if ad:adlist.append(ad)
            producer.PushDataList(kafkaTopicName,adlist)
            ads  = [ParsePap(ad) for ad in adlist]
            producer.PushDataList(commanPattern,ads)

            final = ""
            # for da in adsdata:
            #     final+=json.dumps(da)+"\n"
            # with open(f"outputfilenamethis.json" , "a") as file:
            #     file.write(final)
    def Crawl(self,typ):
            latestad = self.GetLatestad(self.parameter)
            self.createNewUpdate(typ,latestad)
            dic = self.parameter
            dic['produit']=typ
            iniinterval = [0,432]
            maxprize = 250000000
            finalresult = 0
            while iniinterval[1]<=maxprize:
                dic['prix[min]'],dic['prix[max]'] = iniinterval
                response = self.fetchJson(self.apiurl, params=dic)
                totalresult = self.getTotalResult(response)
                # self.driver.get("https://httpbin.org/ip")
                if totalresult!=0 and totalresult<200:
                    print("condition is stisfy going to next interval",totalresult)
                    self.save(response)
                    print(iniinterval,">apending")
                    # filterurllist.append(iniinterval)
                    iniinterval[0] = iniinterval[1]+1
                    iniinterval[1] = iniinterval[0]+int(iniinterval[0]/10)
                    finalresult +=totalresult
                elif totalresult == 0:
                    print("elif 1",iniinterval)
                    last = 10
                    iniinterval[1] = iniinterval[1] + int(iniinterval[1]/last)
                else:
                    print("elif 2",iniinterval)
                    last = -5
                    dif = iniinterval[1]-iniinterval[0]
                    iniinterval[1] = iniinterval[1] + int(dif/-2) 
                    if iniinterval[0]>iniinterval[1]:
                        iniinterval[1] = iniinterval[0]+10
            print(f"total {finalresult} ads fetched")
            return True
    def close(self):
        self.__del__()
    def __del__(self):
        self.startThread = False
        # self.driver.close()
        print("proxy thread is terminated")
dic = {
        "type":"recherche",
        "produit":"vente",
        "geo[ids][]":"25",
    }
def CheckId(id):
    proxy = {"https":"http://sp30786500:Legals786@eu.dc.smartproxy.com:20000/"}
    headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36',
                }
    url = f"https://www.pap.fr/r{id}"
    r = requests.get(url,headers=headers,proxies=proxy)
    if str(id) in r.url:
        return True
    else:False
def pap_scraper(payload):
    print(payload)
    typ = payload.get("real_state_type")
    if typ == "Updated/Latest Ads":
        UpdatePap()
        return 0
    if typ=="rental":typ="location"
    else: typ = "vente"
    ob= PapScraper(dic,proxy=False)
    ob.Crawl(typ)
    ob.__del__()
def UpdatePap():
    types = ['location',"vente"]
    ob= PapScraper(dic,proxy=False)
    for typ in types:
        ob.CrawlLatestV2(typ)
    ob.__del__()

if __name__== "__main__":
    typ ="rental"
    if typ=="rental":typ="location"
    else: typ = "vente"
    ob= PapScraper(dic,proxy=False)
    ob.Crawl(typ)
    ob.__del__()
