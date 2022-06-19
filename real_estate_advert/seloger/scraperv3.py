import imp
import requests
import json
import urllib
import os
import traceback
import time
searchurl = 'https://api-seloger.svc.groupe-seloger.com/api/v1/listings/search'
ViewAddUrl = "https://api-seloger.svc.groupe-seloger.com/api/v2/listings/"
resultcounturl = "https://api-seloger.svc.groupe-seloger.com/api/v1/listings/count/"
session = requests.session()
proxy = {'https': 'http://lum-customer-c_5afd76d0-zone-residential:7nuh5ts3gu7z@zproxy.lum-superproxy.io:22225', 'http': 'http://lum-customer-c_5afd76d0-zone-residential:7nuh5ts3gu7z@zproxy.lum-superproxy.io:22225'}

try:
    from uploader import AsyncKafkaTopicProducer
except:
    from .uploader import AsyncKafkaTopicProducer
cpath =os.path.dirname(__file__)
kafkaTopicName = "seloger_data_v1"
class SelogerScraper:
    def __init__(self,paremeter) -> None:
        self.paremeter= paremeter
        self.session = requests.Session()
        self.headers = self.init_headers()
        self.producer = AsyncKafkaTopicProducer()
        self.logfile = open(f"{cpath}/error.log",'a')
    def __exit__(self):
        self.logfile.close()
    def init_headers(self):
        try:
            headers = {
                'user-agent': 'okhttp/4.6.0',
                'User-Agent': 'okhttp/4.6.0',
            }
            seloger_token_host = os.environ.get('HS_SELOGER_TOKEN_HOST', 'localhost')
            seloger_token_port = os.environ.get('HS_SELOGER_TOKEN_PORT', '8001')

            SELOGER_SECURITY_URL = "https://api-seloger.svc.groupe-seloger.com/api/security"
            time_token = self.session.get(f"{SELOGER_SECURITY_URL}/register", headers=headers,proxies=proxy).json()
            challenge_url = f"http://{seloger_token_host}:{seloger_token_port}/seloger-auth?{urllib.parse.urlencode(time_token, doseq=False)}"
            token = self.session.get(challenge_url).text
            print(token,"self genrager troe")
            final_token = self.session.get(f"{SELOGER_SECURITY_URL}/challenge",headers={**headers, **{'authorization': f'Bearer {token}'}},proxies=proxy).text[1:-1]

            headers = {
                'accept': 'application/json',
                'user-agent': 'Mobile;Android;SeLoger;6.4.2',
                'authorization': f'Bearer {final_token}',
                'content-type': 'application/json; charset=utf-8'
            }
            print(final_token,"<==========final token")
            return headers
        except Exception as e :
            print("excepition==============>",e)
            traceback.print_exc(file=self.logfile)
            return self.init_headers()
    def fetch(self,url,method = "get",retry=0,**kwargs):
        kwargs['headers'] = self.headers
        kwargs['proxies'] = proxy
        try:
            if method=="post":
                r = self.session.post(url,**kwargs)
            else:
                r = self.session.get(url,**kwargs)
            print(r)
        except Exception as e:
            traceback.print_exc(file=self.logfile)
            print(e)
            time.sleep(1)
            self.session.close()
            self.session = requests.Session()
            if retry<10:
                retry+=1
                return self.fetch(url,method=method,retry=retry,**kwargs)
            else:return None
        if r.status_code!=200:
            print(url)
            print(method)
            print(kwargs)
            self.session.close()
            self.session = requests.Session()
            self.headers = self.init_headers()
            if retry<10:
                retry+=1
                return self.fetch(url,method=method,retry=retry,**kwargs)
            else:return None
        return r
    def GetAdInfo(self,addId:int):
        url = f"{ViewAddUrl}{addId}"
        response = self.fetch(url,verify=False,proxies=proxy)
        try:return response.json()
        except:{}
    def getTotalResult(self,param):
        print(param)
        param = [param['query']]
        url = resultcounturl
        r = self.fetch(url,method="post",json=param,proxies=proxy)
        try:
            count = r.json()[0]
        except:
            count = 0
        if count == -1:return 0
        else: return count
    
    def getMaxPrize(self,param):
        # sorting values "sortBy"
        # 1 - prize INCREASING order
        # 2 - prize DECREASING order
        # 10 - DATE NEWEST FIRST
        # 9 - prize OLDEST order
        # 5 - GROWING SURFACE
        # 6 - DECREASING SURFACE
        param["query"]["sortBy"] =2
        # print(param)
        try:
            prize = self.fetch(searchurl,method="post",json=param).json()["items"][0]['price']
            prize = str(prize)
        except:
            prize = "1000"
        maxprice = ''
        for c in prize:
            try:maxprice+=f"{int(c)}"
            except:pass
        if maxprice:return int(maxprice)+1

    def genFilter(self):
        dic = self.paremeter
        totalresult =self.getTotalResult(dic)
        print(totalresult)
        acres = totalresult
        fetchedresult = 0
        iniinterval = [0,1000]
        filterurllist = ''
        finalresult = 0
        maxresult = 50*200
        maxprice = self.getMaxPrize(dic)
        print(maxprice)
        print(totalresult>=maxresult)
        if totalresult>=maxresult:
            while iniinterval[1]<=maxprice:
                dic["query"]['minimumPrice'],dic["query"]['maximumPrice'] = iniinterval
                totalresult = self.getTotalResult(dic)
                if totalresult < maxresult and maxresult-totalresult<=3000:
                    print("condition is stisfy going to next interval")
                    last = 1
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
        print(finalresult,acres)
        filterurllist = [json.loads(query) for query in filterurllist.split("/n/:")]
        return filterurllist
    def Crawlparam(self,param,allPage = True):
        response = self.fetch(searchurl, method = "post", json=param,verify=False, proxies=proxy)
        print(response.status_code,"+++++++++")
        res = response.json()
        pagecount = res['totalCount']
        ads = res['items']
        totalpage = pagecount/len(ads)
        totalpage = int(totalpage) if int(totalpage)==totalpage else int(totalpage)+1
        print("total page",pagecount)
        print(len(ads),"_total ads+++++++++++++")
        adlist = []
        for ad in ads:
            # time.sleep(2)
            adInfo = self.GetAdInfo(ad["id"])
            adlist.append(adInfo)
            # with open("sampleout4.json",'a') as file:
            #     file.write(json.dumps(adInfo)+"\n")
        self.producer.PushDataList(kafkaTopicName,adlist)
        if allPage:
            for i in range(2,totalpage+1):
                param["pageIndex"] = i
                self.Crawlparam(param,allPage=False)
    def CrawlSeloger(self):
        filterlist= self.genFilter()
        # for Filter in filterlist:
        #     self.Crawlparam(Filter)
def main_scraper(payload):
    data = json.load(open(f"{cpath}/selogerapifilter.json",'r'))
    ob = SelogerScraper(data)
    data  = ob.CrawlSeloger()
    # print(data)
