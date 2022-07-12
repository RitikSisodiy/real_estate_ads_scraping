from ast import excepthandler
from datetime import datetime
from turtle import update
from getChrome import getChromePath
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import json
from urllib.parse import urlencode
chrome = getChromePath()
import os
import zipfile
from selenium.webdriver.common.proxy import Proxy, ProxyType
try:from uploader import AsyncKafkaTopicProducer
except:from .uploader import AsyncKafkaTopicProducer

producer = AsyncKafkaTopicProducer()
manifest_json = """
{
    "version": "1.0.0",
    "manifest_version": 2,
    "name": "Chrome Proxy",
    "permissions": [
        "proxy",
        "tabs",
        "unlimitedStorage",
        "storage",
        "<all_urls>",
        "webRequest",
        "webRequestBlocking"
    ],
    "background": {
        "scripts": ["background.js"]
    },
    "minimum_chrome_version":"22.0.0"
}
"""

background_js = """
var config = {
        mode: "fixed_servers",
        rules: {
        singleProxy: {
            scheme: "http",
            host: "%s",
            port: parseInt(%s)
        },
        bypassList: ["localhost"]
        }
    };

chrome.proxy.settings.set({value: config, scope: "regular"}, function() {});

function callbackFn(details) {
    return {
        authCredentials: {
            username: "%s",
            password: "%s"
        }
    };
}

chrome.webRequest.onAuthRequired.addListener(
            callbackFn,
            {urls: ["<all_urls>"]},
            ['blocking']
);
""" % ("zproxy.lum-superproxy.io", 22225, "lum-customer-c_5afd76d0-zone-residential", "7nuh5ts3gu7z")
kafkaTopicName = "pap_data_v1"
cpath =os.path.dirname(__file__)
class PapScraper:
    def __init__(self,parameter,proxy=None) -> None:
        self.parameter = parameter
        self.apiurl = "https://ws.pap.fr/immobilier/annonces"
        self.proxy = proxy
        self.driver = self.getDriver()
        pass
    def getDriver(self):
        options = Options()
        options.add_argument("--headless")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36")
        if self.proxy:  
            pluginfile = 'proxy_auth_plugin.zip'
            with zipfile.ZipFile(pluginfile, 'w') as zp:
                zp.writestr("manifest.json", manifest_json)
                zp.writestr("background.js", background_js)
            options.add_extension(pluginfile)
        driver = webdriver.Chrome(chrome, chrome_options=options)
        return driver

    def getTotalResult(self,resp):
        # print(resp)
        try:
            ads = resp['_embedded'].get("annonce")
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
        content = self.driver.page_source
        content = self.driver.find_element_by_tag_name('pre').text
        parsed_json = json.loads(content)
        return parsed_json
    def save(self,data,getdata=False):
        ads = data['_embedded']["annonce"]
        tasks = [ad['_links']['self']['href'] for ad in ads]
        adlist = []
        for ad in ads:
            adurl = ad['_links']['self']['href']
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
        lastupd[typ]={
                "timestamp": nowtime.timestamp(),
                "lastupdate": latestad['date_classement'],
                "lastadId": latestad.get("id"),
                "source": latestad["_links"].get("desktop")
            }
        with open(f"{cpath}/lastUpdate.json",'w') as file:
            file.write(json.dumps(lastupd))
    def GetLatestad(self,param):
        latestad = self.fetchJson(self.apiurl,params=param)
        latestad = latestad['_embedded'].get("annonce")[0]
        ad = self.fetchJson(latestad['_links']['self']['href'])
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
            dic['recherche[produit]']=typ
            iniinterval = [0,432]
            maxprize = 250000000
            finalresult = 0
            while iniinterval[1]<=maxprize:
                dic['recherche[prix][min]'],dic['recherche[prix][max]'] = iniinterval
                response = self.fetchJson('https://ws.pap.fr/immobilier/annonces', params=dic)
                totalresult = self.getTotalResult(response)
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
        self.driver.close()
def getParams():
    with open(f"{cpath}/params.json",'r') as file:
        data = json.load(file)
    return data
dic = getParams()
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