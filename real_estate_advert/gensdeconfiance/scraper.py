from HttpRequest.requestsModules import okHTTpClient

import os,json


class gensdeconfiance(okHTTpClient):
    def __init__(self, proxyThread=True, proxies=..., aio=True,params={}) -> None:
        cpath = os.path.dirname(__file__)
        URL = "https://www.pap.fr"
        self.params = params
        proxyheaders = {}
        headers = {
            "accept": "application/json",
            "cache-control": "no-cache",
            "uniquedeviceid": "51268a31a20336c9",
            "appversion": "1.71.2.3200232",
            "apporigin": "android",
            "authorization": "undefined",
            "accept-language": "en-US",
            "apptoken": "524b9c59052b5c91f5e78e282ac46b71",
            "appvanity": "6362b08118810",
            "Host": "gensdeconfiance.com",
            "Connection": "Keep-Alive",
            "Accept-Encoding": "gzip",
            "User-Agent": "okhttp/4.9.2"
        }
        super().__init__(proxyThread, URL, headers, proxyheaders, proxies, aio, cpath)
        self.apiurl = "https://gensdeconfiance.com/api/v2/classified"
    def getAdDetails(self,id):
        r= self.fetchJson(f"{self.apiurl}/{id}")
        print(r)
        return r
    def saveAdList(self,adsdata):
        # producer.PushDataList(kafkaTopicName,adsdata)
        final = ""
        for da in adsdata:
            final+=json.dumps(da)+"\n"
        with open(f"out.json" , "a") as file:
            file.write(final)
    def save(self,data,getdata=False):
        ads = data
        newads = []
        for ad in ads:
            addetails = self.getAdDetails(ad["uuid"])
            newads.append({
                **ad,
                **addetails
            })
        self.saveAdList(newads)
    def crawl(self):
        response = self.get(self.apiurl, params=self.params)
        while response: 
            self.save(response)
            self.params["page"]+=1
            print(f"getting {self.params['page']}")
            response = self.get(self.apiurl, params=self.params)
            break
    



if __name__=="__main__":
    params  = {
    "category":"realestate",
    "orderColumn":"displayDate",
    "orderDirection":"DESC",
    "type":"offering",
    "rootLocales[]":"fr",
    "hidden":"true",
    "page":1,
    "itemsPerPage":10
    }
    ob = gensdeconfiance(True,aio=False,params=params)
    ob.crawl()