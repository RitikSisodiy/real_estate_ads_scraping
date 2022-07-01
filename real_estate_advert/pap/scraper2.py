import random
from tabnanny import check
import threading
from seleniumwire import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from urllib.parse import urlencode
import json,os,time
from datetime import datetime

try:from scrapProxy import ProxyScraper
except:from .scrapProxy import ProxyScraper
try:from uploader import AsyncKafkaTopicProducer
except:from .uploader import AsyncKafkaTopicProducer
producer = AsyncKafkaTopicProducer()
kafkaTopicName = "pap_data_v1"
cpath =os.path.dirname(__file__)
chrome = ChromeDriverManager().install()
class PapScraper:
    def __init__(self,parameter,proxy=None) -> None:
        self.parameter = parameter
        self.apiurl = "https://api.pap.fr/app/annonces"
        self.proxy = proxy
        self.proxyUpdateThread()
        SELOGER_SECURITY_URL = "https://www.pap.fr"
        headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36',
                }
        self.prox = ProxyScraper(SELOGER_SECURITY_URL,headers)
        try:
            self.proxies = self.readProxy()
        except:
            self.getProxyList()
        self.driver = self.getDriver()
        self.driver.proxy = self.getRandomProxy()
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
        self.driver.request_interceptor = self.interceptor
        pass
    def getRandomProxy(self):
        proxy = random.choice(self.proxies)
        return proxy
    def proxyUpdateThread(self):
        print("proxy thread is started")
        self.startThread = True
        self.proc = threading.Thread(target=self.updateProxyList, args=())
        self.proc.start()
    def updateProxyList(self,interval=300):
        if self.readProxy():time.sleep(interval)
        while True:
            if not self.startThread:
                break
            self.getProxyList()
            time.sleep(interval)     
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
            try:del request.headers[key] # Delete the header first
            except:pass
            request.headers[key] = val
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
        self.driver.get(url)
        self.driver.save_screenshot("image.png")
        try:
            content = self.driver.page_source
            content = self.driver.find_element_by_tag_name('pre').text
            parsed_json = json.loads(content)
        except Exception as e:
            print("exeptin", e)
            self.driver.proxy = self.getRandomProxy()
            return self.fetchJson(url,params)
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
    def saveAdList(self,adsdata):
            producer.PushDataList(kafkaTopicName,adsdata)
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
    def close(self):
        self.startThread = False
        self.driver.close()
        print("proxy thread is terminated")
dic = {
        "type":"recherche",
        "produit":"vente",
        "geo[ids][]":"25",
        "typesbien[]":"immeuble",
        "prix[min]":"10432",
        "prix[max]":"254579774",
    }
def pap_scraper(payload):
    print(payload)
    typ = payload.get("real_state_type")
    if typ == "Updated/Latest Ads":
        UpdatePap()
    if typ=="rental":typ="location"
    else: typ = "vente"
    ob= PapScraper(dic,proxy=False)
    ob.Crawl(typ)
    ob.close()
def UpdatePap():
    types = ['location',"vente"]
    ob= PapScraper(dic,proxy=False)
    for typ in types:
        ob.CrawlLatest(typ)
    ob.close()

if __name__== "__main__":
    typ ="rental"
    dic = {
        "type":"recherche",
        "produit":"vente",
        "geo[ids][]":"25",
        "typesbien[]":"immeuble",
        "prix[min]":"10432",
        "prix[max]":"254579774",
    }
    if typ=="rental":typ="location"
    else: typ = "vente"
    ob= PapScraper(dic,proxy=False)
    ob.Crawl(typ)
    ob.close()